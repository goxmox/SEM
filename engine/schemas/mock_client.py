from tinkoff.invest import PostOrderResponse, OrderState, \
    GetOrderBookResponse, Order, GetOrdersResponse

import engine.schemas.datatypes
from api.tinvest.tperiod import TPeriod
from api.tinvest.tticker import TTicker
from api.tinvest.datatypes import SessionAuction
from api.tinvest.utils import to_quotation
import api.tinvest.tclient as t_api
from api.broker_list import t_invest
from engine.schemas.client import Client
from engine.schemas.datatypes import Ticker, Period
from engine.schemas.pipeline import Pipeline
from engine.schemas.enums import OrderDirection, OrderExecutionReportStatus, SessionPeriod, OrderType
from engine.candles.candles_uploader import LocalTSUploader
import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta

from abc import ABC, abstractmethod



class MockClient(Client, ABC):
    def __init__(
            self,
            period: datetime,
            tickers: list[Ticker],
            bid_orderbook_price: str = 'low',
            ask_orderbook_price: str = 'high',
            market_order_price: str = 'open',
            buy_price_end_period: str = 'low',
            sell_price_end_period: str = 'high',
            lag_in_cached_candles: int = 1,
            cash: float = 100000
    ):
        super().__init__()

        self.bid_orderbook_price = bid_orderbook_price
        self.ask_orderbook_price = ask_orderbook_price
        self.market_order_price = market_order_price
        self.buy_price_end_period = buy_price_end_period
        self.sell_price_end_period = sell_price_end_period
        self.lag_in_cached_candles = lag_in_cached_candles

        self.period: Period = Period(time_period=period)
        self.period_duration = 0
        self.services: MockClientServices = None
        self._cash = cash

        self.uid_to_tickers = {ticker.uid: ticker for ticker in tickers}

        self.candle_data: dict = {
            ticker:
                LocalTSUploader.download_ts(ticker=ticker) for ticker in tickers
        }
        self.current_candles = {ticker: {} for ticker in self.candle_data.keys()}
        self.last_candles_idx = {ticker: 0 for ticker in self.candle_data.keys()}

        for ticker, candles_df in self.candle_data.items():
            self.last_candles_idx[ticker] = (candles_df.index <= self.period.time_period).argmin() - 1
            self.current_candles[ticker] = candles_df.iloc[self.last_candles_idx[ticker]].to_dict()

        for ticker in self.candle_data.keys():
            LocalTSUploader.candles_in_memory[ticker] = \
                self.candle_data[ticker].iloc[:self.last_candles_idx[ticker] + 1]

            LocalTSUploader.last_candles[ticker] = \
                self.candle_data[ticker].iloc[self.last_candles_idx[ticker]:self.last_candles_idx[ticker] + 1]

            LocalTSUploader.candles_start_dates[ticker] = \
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
                mid = (round(Decimal(self.current_candles[ticker]['high']), 9)
                       + round(Decimal(self.current_candles[ticker]['low']), 9) ) / 2

                if self.buy_price_end_period == 'mid':
                    p_buy = mid
                else:
                    p_buy = round(Decimal(self.current_candles[ticker][self.buy_price_end_period]), 9)

                if self.sell_price_end_period == 'mid':
                    p_sell = mid
                else:
                    p_sell = round(Decimal(self.current_candles[ticker][self.sell_price_end_period]), 9)

                if self.market_order_price == 'mid':
                    p_market = mid
                else:
                    p_market = round(Decimal(self.current_candles[ticker][self.market_order_price]), 9)

                if order.order_type == OrderType.ORDER_TYPE_LIMIT:
                    if ((order.direction == OrderDirection.ORDER_DIRECTION_BUY and p >= p_buy)
                            or (order.direction == OrderDirection.ORDER_DIRECTION_SELL and p <= p_sell)):
                        order.status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                        order.executed_order_price = p
                        order.lots_executed = order.quantity
                        order.executed_commission = Decimal(0)
                        order.total_order_amount = order.quantity * p * ticker.lot

                        self._cash += float(order.quantity * p * ticker.lot
                                            * (-1 if order.direction == OrderDirection.ORDER_DIRECTION_BUY else 1))
                elif order.order_type == OrderType.ORDER_TYPE_MARKET:
                    order.status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                    order.executed_order_price = p_market
                    order.lots_executed = order.quantity
                    order.executed_commission = Decimal(0)
                    order.total_order_amount = order.quantity * p_market * ticker.lot

                    self._cash += float(order.quantity * p_market * ticker.lot
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

    @abstractmethod
    def ready_to_trade(self, sessions, types_instruments,
                       include_opening=True, include_closing=True):
        pass


    @staticmethod
    @abstractmethod
    def price_correction(price, ticker) -> Decimal:
        pass

    @staticmethod
    @abstractmethod
    def lots_correction(portfolio_lots, ticker) -> int:
        pass


class MockClientServices(local_api.Services):
    def __init__(self, client: TMockClient):
        self.client = client
        self.orders: MockOrders = MockOrders(self)
        self.market_data: MockMarketData = MockMarketData(self)
        self.last_cached_candles_idx: dict[str, int] = client.last_candles_idx.copy()

        for last_cached_candle_key in self.last_cached_candles_idx.keys():
            self.last_cached_candles_idx[last_cached_candle_key] -= self.client.lag_in_cached_candles

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

        LocalTSUploader.save_new_observations(new_candles, ticker)

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
    order_type: OrderType
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
                      order_type=order_type,
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

        mid = (self.client.current_candles[ticker]['low'] + self.client.current_candles[ticker]['high']) / 2

        if self.client.bid_orderbook_price == 'mid':
            p_bid = mid
        else:
            p_bid = self.client.current_candles[ticker][self.client.bid_orderbook_price]

        if self.client.ask_orderbook_price == 'mid':
            p_ask = mid
        else:
            p_ask = self.client.current_candles[ticker][self.client.ask_orderbook_price]

        return local_api.GetOrderBookResponse(
            bids=[local_api.Order(price=p_bid,
                                  quantity=1000000)],
            asks=[local_api.Order(price=p_ask,
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
