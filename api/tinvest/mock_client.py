from tinkoff.invest import PostOrderResponse, OrderState, \
    GetOrderBookResponse, Order, GetOrdersResponse

import engine.schemas.datatypes
from api.tinvest.tperiod import TPeriod
from api.tinvest.tticker import TTicker
from api.tinvest.datatypes import SessionAuction
from api.tinvest.utils import to_quotation
import api.tinvest.tclient as t_api
from api.broker_list import t_invest
import engine.schemas.client as local_api
from engine.schemas.data_broker import DataTransformerBroker
from engine.schemas.enums import OrderDirection, OrderExecutionReportStatus, SessionPeriod, OrderType
from engine.candles.candles_uploader import LocalCandlesUploader
import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta


class TMockClient(local_api.Client):
    def __init__(
            self,
            period: datetime,
            tickers: list[TTicker],
            cash: float = 100000
    ):
        super().__init__(t_invest)

        self.period: TPeriod = TPeriod(time_period=period)
        self.period_duration = 0
        self.services: MockClientServices = None
        self._cash = cash

        self.uid_to_tickers = {ticker.uid: ticker for ticker in tickers}

        self.candle_data: dict = {
            ticker:
                LocalCandlesUploader.upload_candles(ticker=ticker) for ticker in tickers
        }
        self.current_candles = {ticker: {} for ticker in self.candle_data.keys()}
        self.last_candles_idx = {ticker: 0 for ticker in self.candle_data.keys()}

        for ticker, candles_df in self.candle_data.items():
            self.last_candles_idx[ticker] = (candles_df.index <= self.period.time_period).argmin() - 1
            self.current_candles[ticker] = candles_df.iloc[self.last_candles_idx[ticker]].to_dict()

        for ticker in self.candle_data.keys():
            LocalCandlesUploader.candles_in_memory[ticker] = \
                self.candle_data[ticker].iloc[:self.last_candles_idx[ticker] + 1]

            LocalCandlesUploader.last_candles[ticker] = \
                self.candle_data[ticker].iloc[self.last_candles_idx[ticker]:self.last_candles_idx[ticker] + 1]

            LocalCandlesUploader.candles_start_dates[ticker] = \
                self.period.time_period + timedelta(minutes=1)

    def __enter__(self):
        self.services = MockClientServices(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def next_period(self):
        for order in self.services.orders.order_history:
            ticker = self.uid_to_tickers[order.instrument_id]

            if (self.period.instrument_session[ticker.type_instrument]
                    == SessionPeriod.CLOSED):
                continue

            if order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
                p = round(order.price, 9)
                low = round(Decimal(self.current_candles[ticker]['low']), 9)
                high = round(Decimal(self.current_candles[ticker]['high']), 9)

                if ((order.direction == OrderDirection.ORDER_DIRECTION_BUY and p >= low)
                        or (order.direction == OrderDirection.ORDER_DIRECTION_SELL and p <= high)):
                    order.status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                    order.executed_order_price = p
                    order.lots_executed = order.quantity
                    order.executed_commission = Decimal(0)
                    order.total_order_amount = order.quantity * p * ticker.lot

                    self._cash += float(order.quantity * p * ticker.lot
                                        * (-1 if order.direction == OrderDirection.ORDER_DIRECTION_BUY else 1))

        for ticker, candles_df in self.candle_data.items():
            if self.period.instrument_session[ticker.type_instrument] == SessionPeriod.CLOSED:
                continue

            self.last_candles_idx[ticker] += 1
            self.current_candles[ticker] = candles_df.iloc[self.last_candles_idx[ticker]].to_dict()

        self.period.next_period(update_with_cur_time=False)

    def get_account(self, account_type):
        return MockUsers(self.services).account

    def get_available_balance(self, account):
        return self._cash

    def ready_to_trade(self, sessions, types_instruments,
                       include_opening=True, include_closing=True):
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
        return t_api.TClient.price_correction(price, ticker)

    @staticmethod
    def lots_correction(portfolio_lots, ticker) -> int:
        return t_api.TClient.lots_correction(portfolio_lots, ticker)


class MockClientServices(local_api.Services):
    def __init__(self, client: TMockClient):
        self.client = client
        self.orders: MockOrders = MockOrders(self)
        self.market_data: MockMarketData = MockMarketData(self)
        self.last_cached_candles_idx: dict[str, int] = client.last_candles_idx.copy()

    def get_instruments(self):
        pass

    def get_candles(
            self,
            ticker: TTicker,
            start_date: Optional[datetime] = None
    ):
        new_candles = self.client.candle_data[ticker].iloc[
                      self.last_cached_candles_idx[ticker] + 1: self.client.last_candles_idx[ticker] + 1
                      ]

        self.last_cached_candles_idx[ticker] = self.client.last_candles_idx[ticker]

        LocalCandlesUploader.save_new_candles(new_candles, ticker)

        return len(new_candles) > 0

    def _candles_writer(
            self,
            uid: str,
            from_,
            to=None
    ):
        pass


class MockService:
    def __init__(self, services: MockClientServices):
        self.client = services.client


@dataclass
class MockOrder:
    order_id: str
    instrument_id: str
    price: Decimal
    quantity: int
    direction: OrderDirection
    status: OrderExecutionReportStatus
    executed_order_price: Decimal
    lots_executed: int
    executed_commission: Decimal
    total_order_amount: Decimal


class MockOrders(MockService, local_api.OrdersService):
    order_history: list[MockOrder]

    def __init__(self, client):
        super().__init__(client)
        self.order_history = []
        self.id = 0

    def post_order(self, *args, quantity: int = 0, price: Decimal = None,
                   direction: OrderDirection = OrderDirection.ORDER_DIRECTION_UNSPECIFIED,
                   account_id: str = "", order_type: OrderType = OrderType.ORDER_TYPE_UNSPECIFIED,
                   order_id: str = "", instrument_id: str = "") -> local_api.PostOrderResponse:
        self.order_history.append(
            MockOrder(order_id=str(self.id),
                      instrument_id=instrument_id,
                      price=price, quantity=quantity,
                      direction=direction,
                      status=OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW,
                      executed_order_price=None, lots_executed=None,
                      executed_commission=None, total_order_amount=None)
        )

        self.id += 1

        return PostOrderResponse(
            order_id=str(self.id - 1),
            execution_report_status=OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW,
            initial_order_price=price
        )

    def cancel_order(
            self,
            *,
            account_id: str = "",
            order_id: str = "",
            **kwargs
    ) -> local_api.CancelOrderResponse:
        order_id = int(order_id)

        if self.order_history[order_id].status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL:
            self.order_history[order_id].status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED

    def get_order_state(
            self, *,
            account_id: str = '',
            order_id: str = '',
            **kwargs
    ) -> OrderState:
        order_id = int(order_id)
        order = self.order_history[order_id]

        return OrderState(order_id=order.order_id,
                          execution_report_status=order.status,
                          direction=order.direction,
                          executed_order_price=order.executed_order_price,
                          lots_executed=order.lots_executed,
                          executed_commission=order.executed_commission,
                          total_order_amount=order.total_order_amount)

    def get_orders(
            self, *args,
            account_id: str = '',
            **kwargs
    ):
        return GetOrdersResponse(orders=self.order_history)

    def replace_order(self, *args, **kwargs):
        pass


class MockMarketData(MockService, local_api.MarketDataService):
    def get_candles(self, *args, **kwargs):
        pass

    def get_order_book(
            self, *,
            depth: int = None,
            instrument_id: str = "",
            **kwargs
    ) -> local_api.GetOrderBookResponse:
        ticker = self.client.uid_to_tickers[instrument_id]

        mid_price = (self.client.current_candles[ticker]['low'] + self.client.current_candles[ticker]['high']) / 2
        open_price = self.client.current_candles[ticker]['open']

        return local_api.GetOrderBookResponse(
            bids=[local_api.Order(price=open_price,
                                  quantity=1000000)],
            asks=[local_api.Order(price=open_price,
                                  quantity=1000000)],
            depth=1,
            instrument_uid=''
        )


@dataclass
class MockAccount:
    id: str


class MockUsers(MockService, local_api.UsersService):
    account: MockAccount = MockAccount(id='0')

    def get_accounts(self):
        return self.account


class MockOperations(MockService, local_api.OperationsService):
    pass
