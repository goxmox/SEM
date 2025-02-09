from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from engine.schemas.datatypes import Period, Ticker
from engine.strategies.datatypes import OrderManager
from engine.schemas.client import Client, Services, Account, OrderState
from engine.schemas.enums import SessionPeriod, OrderExecutionReportStatus, OrderDirection
from engine.schemas.constants import log_path


class Strategy(ABC):
    def __init__(self, cash_share: float = 1.,
                 sessions=(SessionPeriod.MAIN,), **additional_open_to_trading_parameters):
        self.active, self._executed, self.ongoing_trading = True, False, False
        self._period: Period = None
        self._account: Account = None
        self._cash = None
        self._client: Client = None
        self._services: Services = None
        self._cash_share = cash_share
        self._sessions = sessions
        self._additional_open_to_trading_parameters = additional_open_to_trading_parameters
        self.tickers_collection: list[Ticker] = None
        self.portfolio_prices: dict[Ticker, dict[str, float]]
        self.portfolio_lots: dict[Ticker, int] = {}
        self._order_manager: OrderManager = None
        self._tickers_for_candle_fetching = None
        self._selected_tickers_to_trade: set = set()
        self._selected_tickers_to_buy: set = set()
        self._selected_tickers_to_sell: set = set()
        self.types_instruments = None

    # in _update these two methods should be called before calling _compute_portfolio or _upload_candles
    # in order to update the list of tickers for which prices/candles will be fetched
    def _set_tickers_for_candle_fetching(self):
        self._tickers_for_candle_fetching = self.tickers_collection

    def _get_candles(self):
        for ticker in self._tickers_for_candle_fetching:
            self._services.get_candles(ticker)

    def _get_new_prices(self, depth=1) -> dict[str, dict[str, np.array]]:
        prices = {}

        for ticker in self.tickers_collection:
            order_book = self._services.market_data.get_order_book(instrument_id=ticker.uid, depth=depth)
            bid = [order_book.bids[i].price for i in range(depth)]
            ask = [order_book.asks[i].price for i in range(depth)]

            prices[ticker] = {'to_buy': np.array(bid), 'to_sell': np.array(ask)}

        return prices

    @abstractmethod
    def _determine_lots(self, ticker: Ticker):
        pass

    @abstractmethod
    def _set_portfolio_prices(self, new_data) -> dict[Ticker, dict[str, float]]:
        pass

    def _compute_portfolio_prices(self):
        new_prices = self._get_new_prices()
        self.portfolio_prices = self._set_portfolio_prices(new_prices)

    def _compute_portfolio_lots_for_trade(self, ticker: Ticker):
        self.portfolio_lots[ticker] = self._determine_lots(ticker)

    # orders is a list of tuples with a name of an order at index 0, and a metadata at index 1

    def _trade(self):
        for order in self._order_manager.extract_new_orders():
            if order.status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_UNSPECIFIED:
                continue

            order_response = self._services.orders.post_order(
                instrument_id=order.instrument_uid,
                price=order.price,
                quantity=order.lots,
                direction=order.direction,
                account_id=order.account_id,
                order_type=order.order_type
            )

            order.status = order_response.execution_report_status
            order.order_id = order_response.order_id
            #order.commission = order_response.commission
            order.price = float(order_response.initial_order_price)

    def execute(self, client: Client, account, tickers: list[Ticker]):
        if not self._executed:
            self._client = client
            self._services = client.services
            self._period = client.period
            self._account = client.get_account(account)
            self.tickers_collection = tickers
            self._tickers_for_candle_fetching = tickers
            self.types_instruments = list(set([ticker.type_instrument for ticker in self.tickers_collection]))
            self._order_manager = OrderManager(
                client=client,
                account=self._account,
                tickers_collection=self.tickers_collection
            )

        self._cash = client.get_available_balance(self._account) * self._cash_share

        self.ongoing_trading = client.ready_to_trade(self._sessions, self.types_instruments,
                                                     **self._additional_open_to_trading_parameters)

        if self.ongoing_trading:
            self._update()

    @abstractmethod
    def _update(self):
        pass

    def terminate(self):
        for order in self._services.orders.get_orders(account_id=self._account.id).orders:
            self._services.orders.cancel_order(account_id=self._account.id, order_id=order.order_id)

        if len(self._order_manager.transactions) > 0:
            pd.DataFrame(self._order_manager.transactions).T.to_csv(
                log_path + f'strategy_result.csv', mode='a', header=False)

        self.active = False
