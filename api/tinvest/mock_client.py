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
from engine.schemas.mock_client import MockClient
from engine.schemas.pipeline import Pipeline
from engine.schemas.enums import OrderDirection, OrderExecutionReportStatus, SessionPeriod, OrderType
from engine.candles.candles_uploader import LocalTSUploader
import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta


class TMockClient(MockClient):
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
