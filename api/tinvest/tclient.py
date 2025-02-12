import tinkoff.invest as t_api
import tinkoff.invest.services as t_services
import engine.schemas.client as local_api
import pandas as pd

from engine.schemas.datatypes import Broker
from engine.schemas.constants import instrument_path
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX
from tinkoff.invest import CandleInterval, RequestError
from api.tinvest.tperiod import TPeriod
from api.tinvest.tticker import TTicker
from api.tinvest.datatypes import SessionAuction, AccountType, InstrumentType
from api.tinvest.utils import quotation_to_float, quotation_to_decimal, to_quotation
from api.broker_list import t_invest
from typing_extensions import Self
from decimal import Decimal
from dataclasses import asdict
from datetime import datetime, timedelta
from time import sleep
import os


class TClient(t_api.Client, local_api.Client):
    services: 'TServices'
    period: TPeriod
    sandbox: bool
    broker: Broker = t_invest

    def __init__(
            self,
            sandbox: bool = True,
            restart_sandbox_account: bool = False,
            trade: bool = False,
            period_duration: int = 5,
            **kwargs
    ):
        target = INVEST_GRPC_API

        if sandbox:
            target = INVEST_GRPC_API_SANDBOX
            token = os.environ.get('TOKEN_SANDBOX')
        elif trade:
            token = os.environ.get('TOKEN_TRADE')
        else:
            token = os.environ.get("TOKEN_NO_TRADE")

        super().__init__(token=token, target=target, **kwargs)
        self.period = TPeriod()
        self._period_duration = period_duration
        self.TickerWrapper = TTicker
        self.sandbox = sandbox
        self._restart_sandbox_account = restart_sandbox_account

    def __enter__(self) -> Self:
        channel = self._channel.__enter__()
        self.services = TServices(
            channel,
            token=self._token,
            sandbox_token=self._sandbox_token,
            app_name=self._app_name,
        )

        if self.sandbox and self._restart_sandbox_account:
            accounts = self.services.users.get_accounts().accounts

            if len(accounts) > 0:
                for account in accounts:
                    self.services.sandbox.close_sandbox_account(account_id=account.id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        return False

    def next_period(self):
        self.period.next_period(update_with_cur_time=True)

    def get_account(self, account_type: AccountType):
        accounts = self.services.users.get_accounts().accounts

        for account in accounts:
            if account.type == account_type.value:
                return account
        else:
            if self.sandbox:
                self.services.sandbox.open_sandbox_account(name='sandbox')
                account = self.services.users.get_accounts().accounts[0]

                self.services.sandbox.sandbox_pay_in(
                    account_id=account.id,
                    amount=t_api.MoneyValue(
                        currency='RUB',
                        units=1000000,
                        nano=0)
                )

                return account
            else:
                raise ValueError(f'There is no account of type {AccountType.name}.')

    def get_available_balance(self, account) -> float:
        return quotation_to_float(self.services.operations.get_portfolio(
            account_id=account.id).total_amount_currencies)

    def ready_to_trade(
            self,
            sessions, types_instruments,
            include_opening=True,
            include_closing=True
    ):
        for type_instrument in types_instruments:
            if not self.period.instrument_session[type_instrument] in sessions:
                return False
            elif self.period.instrument_auction[type_instrument] == SessionAuction.OPENING and not include_opening:
                return False
            elif self.period.instrument_auction[type_instrument] == SessionAuction.CLOSING and not include_closing:
                return False
        else:
            return True

    @staticmethod
    def price_correction(price, ticker) -> Decimal:
        min_incr = round(Decimal(ticker.min_price_increment), 9)
        price = round(Decimal(price), 9)

        return price - (price % min_incr)

    @staticmethod
    def lots_correction(portfolio_lot, ticker) -> int:
        if portfolio_lot > 0:
            return int(portfolio_lot // ticker.lot)
        else:
            return int(abs(portfolio_lot + (portfolio_lot % ticker.lot)) // ticker.lot)


class TServices(t_services.Services, local_api.Services):
    candle_query_period = 365
    broker: Broker = t_invest

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.orders = TOrdersService(self.orders)
        self.market_data = TMarketDataService(self.market_data)

    def get_instruments(self):
        for instrument_type in InstrumentType:
            if instrument_type == InstrumentType.STOCK:
                unitnano_fields = ['min_price_increment', 'nominal', 'dshort_min', 'dlong_min',
                                   'dshort', 'dlong', 'kshort', 'klong']
                instruments_data = self.instruments.shares().instruments
            elif instrument_type == InstrumentType.FUTURES:
                unitnano_fields = ['min_price_increment', 'basic_asset_size', 'dshort_min', 'dlong_min',
                                   'dshort', 'dlong', 'kshort', 'klong', 'initial_margin_on_buy',
                                   'initial_margin_on_sell', 'min_price_increment_amount']
                instruments_data = self.instruments.futures().instruments
            else:
                raise ValueError(f'Instrument type {instrument_type} is not supported.')

            df_instruments_info = None

            for instrument in instruments_data:
                # leaving only russian stocks which are tradable via this api
                if (instrument.api_trade_available_flag
                        and instrument.country_of_risk == 'RU'
                        and '@' not in instrument.ticker
                        and instrument.currency == 'rub'):
                    # converting from dataclass to a workable dict
                    instrument = asdict(instrument)

                    del instrument['brand']

                    # these fields are themselves dicts, cause trouble converting to dataframe
                    for field in unitnano_fields:
                        instrument[field] = quotation_to_float(instrument[field])
                    # making this dict convertable to a dataframe
                    instrument = {field: [value] for field, value in zip(instrument.keys(), instrument.values())}

                    if df_instruments_info is None:
                        df_instruments_info = pd.DataFrame.from_dict(instrument)
                    else:
                        df_instruments_info = pd.concat([df_instruments_info, pd.DataFrame(instrument)])

            if not os.path.isdir(instrument_path + f'{t_invest.broker_name}\\'):
                os.mkdir(instrument_path + f'{t_invest.broker_name}\\')

            df_instruments_info.to_csv(
                instrument_path + f'{t_invest.broker_name}\\{instrument_type.name}.csv',
                encoding='utf-8-sig',
                index=False
            )

    # candle frequency is hardcoded at 1min for now
    def _candles_writer(
            self,
            uid: str,
            from_: datetime,
            to: datetime = None
    ):
        candle_dict = {'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'time': []}
        resource_exhausted = False

        while not resource_exhausted:
            try:
                print(from_)
                if to is not None:
                    to_ = min(to, from_ + timedelta(days=self.candle_query_period), datetime.now().astimezone())
                else:
                    to_ = min(from_ + timedelta(days=self.candle_query_period), datetime.now().astimezone())

                resource_exhausted = True
                for candle in self.get_all_candles(
                        instrument_id=uid,
                        from_=from_,
                        to=to_,
                        interval=CandleInterval.CANDLE_INTERVAL_1_MIN
                ):
                    resource_exhausted = False

                    candle_dict['open'].append(quotation_to_float(candle.open))
                    candle_dict['high'].append(quotation_to_float(candle.high))
                    candle_dict['low'].append(quotation_to_float(candle.low))
                    candle_dict['close'].append(quotation_to_float(candle.close))
                    candle_dict['volume'].append(candle.volume)
                    candle_dict['time'].append(candle.time)

                from_ = min(from_ + timedelta(days=self.candle_query_period, minutes=1), datetime.now().astimezone())
            except RequestError as e:
                print(e.args[0], e.args[1], e.args[2])
                resource_exhausted = False
                sleep(e.args[2].ratelimit_reset + 0.5)

                if len(candle_dict['open']) > 0:
                    from_ = candle_dict['time'][-1] + timedelta(minutes=1)

        return pd.DataFrame(candle_dict)


class TOrder(t_api.Order, local_api.Order):
    def __init__(self, order: t_api.Order):
        copy_attributes(self, order)

        self.price: float = quotation_to_float(self.price)


class TGetOrderBookResponse(t_api.GetOrderBookResponse, local_api.GetOrderBookResponse):
    def __init__(self, orderbook_response: t_api.GetOrderBookResponse):
        copy_attributes(self, orderbook_response)

        self.asks = [TOrder(order) for order in self.asks]
        self.bids = [TOrder(order) for order in self.bids]


class TMarketDataService(t_services.MarketDataService, local_api.MarketDataService):
    def __init__(self, market_data):
        copy_attributes(self, market_data)

    def get_order_book(
            self, *args, **kwargs
    ) -> TGetOrderBookResponse:
        return TGetOrderBookResponse(super().get_order_book(*args, **kwargs))


class TPostOrderResponse(t_api.PostOrderResponse):
    def __init__(self, order_response: t_api.OrderState):
        copy_attributes(self, order_response)

        self.initial_order_price: Decimal = quotation_to_decimal(order_response.initial_order_price)
        self.total_order_amount: Decimal = quotation_to_decimal(order_response.total_order_amount)
        self.executed_order_price: Decimal = quotation_to_decimal(order_response.executed_order_price)
        self.executed_commission: Decimal = quotation_to_decimal(order_response.executed_commission)


class TOrderState(t_api.OrderState):
    def __init__(self, order_state: t_api.OrderState):
        copy_attributes(self, order_state)

        self.total_order_amount: Decimal = quotation_to_decimal(order_state.total_order_amount)
        self.executed_order_price: Decimal = quotation_to_decimal(order_state.executed_order_price)
        self.executed_commission: Decimal = quotation_to_decimal(order_state.executed_commission)


class TOrdersService(t_services.OrdersService, local_api.OrdersService):
    def __init__(self, orders_service):
        copy_attributes(self, orders_service)

    def post_order(
            self, *args, price: Decimal = None, **kwargs
    ) -> TPostOrderResponse:
        price = to_quotation(price)

        return TPostOrderResponse(super().post_order(*args, price=price, **kwargs))

    def get_order_state(self, *args, **kwargs) -> TOrderState:
        return TOrderState(super().get_order_state(*args, **kwargs))


def copy_attributes(new_obj, obj):
    for attr in obj.__dict__:
        new_obj.__dict__[attr] = obj.__dict__[attr]
