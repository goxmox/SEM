from abc import ABC, abstractmethod
from typing_extensions import Self
from typing import Callable, Optional
from decimal import Decimal
from engine.schemas.enums import OrderExecutionReportStatus, OrderDirection, OrderType
from engine.schemas.datatypes import Period, Ticker, Broker
from engine.transformers.candles_processing import CandlesRefinerTransformer
from engine.candles.candles_uploader import LocalCandlesUploader
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


class Client(ABC):
    broker: 'Broker'

    def __init__(self, *args, **kwargs):

        self.services: 'Services' = None
        self.period: Period = None
        self.TickerWrapper: Callable[[str], Ticker] = None
        self.period_duration: int = None

    @abstractmethod
    def get_account(self, account_type) -> 'Account':
        pass

    @abstractmethod
    def get_available_balance(self, account):
        pass

    @abstractmethod
    def next_period(self):
        pass

    @abstractmethod
    def ready_to_trade(self, sessions, types_instruments, **kwargs) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def price_correction(price, ticker) -> Decimal:
        pass

    @staticmethod
    @abstractmethod
    def lots_correction(portfolio_lots, ticker) -> int:
        pass

    @abstractmethod
    def __enter__(self) -> Self:
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@dataclass
class Services(ABC):
    instruments: 'InstrumentsService'
    market_data: 'MarketDataService'
    market_data_stream: 'MarketDataStreamService'
    operations: 'OperationsService'
    operations_stream: 'OperationsStreamService'
    orders_stream: 'OrdersStreamService'
    orders: 'OrdersService'
    users: 'UsersService'
    sandbox: 'SandboxService'

    broker: 'Broker'

    @abstractmethod
    def get_instruments(self):
        pass

    @abstractmethod
    def _candles_writer(
            self,
            uid: str,
            from_: datetime,
            to: datetime = None
    ):
        pass

    def get_candles(
            self,
            ticker: 'Ticker',
            start_date: Optional[datetime] = None
    ) -> bool:
        last_cached_candle = LocalCandlesUploader.get_last_candle(ticker)

        if last_cached_candle is not None:
            last_cached_candle = last_cached_candle.copy()

            last_day_number = last_cached_candle.iloc[0]['day_number']
            e
            del last_cached_candle['day_number']
            last_cached_candle = last_cached_candle.reset_index()
        else:
            last_day_number = 0

        if start_date is None:
            start_date = LocalCandlesUploader.get_new_candle_datetime(ticker)

        candles_request_date = datetime.now().astimezone()

        # aborting calls to fetching methods if there are no new candles yet
        if (candles_request_date - start_date).seconds < 60:
            return pd.DataFrame([])

        new_candles = self._candles_writer(
            ticker.uid,
            from_=start_date
        )

        if last_cached_candle is not None:
            if len(new_candles) > 0:
                new_candles = pd.concat([last_cached_candle, new_candles])
            else:
                new_candles = last_cached_candle
        elif new_candles.shape[0] == 0:
            # no data whatsoever on this ticker
            return False

        new_candles = CandlesRefinerTransformer(
            broker=self.broker,
            ticker=ticker,
            candles_request_date=candles_request_date,
            last_day_number=last_day_number
        ).fit_transform(new_candles)

        if last_cached_candle is not None:
            new_candles = new_candles.iloc[1:]

        if new_candles.shape[0] != 0:
            LocalCandlesUploader.save_new_candles(new_candles, ticker)

            return True
        else:
            return False



# instruments service


class InstrumentsService(ABC):
    @abstractmethod
    def shares(self, *args, **kwargs):
        pass

    @abstractmethod
    def bonds(self, *args, **kwargs):
        pass

    @abstractmethod
    def futures(self, *args, **kwargs):
        pass

    @abstractmethod
    def currencies(self, *args, **kwargs):
        pass

    @abstractmethod
    def etfs(self, *args, **kwargs):
        pass


# market-data service


class MarketDataService(ABC):
    @abstractmethod
    def get_candles(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_order_book(
            self, *args,
            depth: int = None,
            instrument_id: str = '',
            **kwargs
    ) -> 'GetOrderBookResponse':
        pass


@dataclass
class GetOrderBookResponse(ABC):
    depth: int
    bids: list['Order']
    asks: list['Order']
    instrument_uid: str


@dataclass
class Order(ABC):
    price: float
    quantity: int


# market-data stream service


class MarketDataStreamService(ABC):
    @abstractmethod
    def market_data_stream(self, *args, **kwargs):
        pass


# operations service


class OperationsService(ABC):
    @abstractmethod
    def get_operations(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_portfolio(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_positions(self, *args, **kwargs):
        pass


# operations stream service


class OperationsStreamService(ABC):
    @abstractmethod
    def portfolio_stream(self, *args, **kwargs):
        pass


# orders service


class OrdersService(ABC):
    @abstractmethod
    def post_order(
            self, *args,
            instrument_id: str = None,
            price: Decimal = None,
            quantity: int = 0,
            direction: OrderDirection = OrderDirection.ORDER_DIRECTION_UNSPECIFIED,
            account_id: str = '',
            order_type: OrderType = OrderType.ORDER_TYPE_UNSPECIFIED,
            **kwargs
    ) -> 'PostOrderResponse':
        pass

    @abstractmethod
    def cancel_order(
            self, *args,
            account_id: str = '',
            order_id: str = '',
            **kwargs
    ) -> 'CancelOrderResponse':
        pass

    @abstractmethod
    def get_order_state(
            self, *args,
            account_id: str = '',
            order_id: str = '',
            **kwargs
    ) -> 'OrderState':
        pass

    @abstractmethod
    def get_orders(
            self, *args,
            account_id: str = '',
            **kwargs
    ) -> 'GetOrdersResponse':
        pass

    @abstractmethod
    def replace_order(self, *args, **kwargs):
        pass


@dataclass
class OrderState(ABC):
    execution_report_status: OrderExecutionReportStatus
    order_id: str
    executed_order_price: Decimal
    total_order_amount: Decimal
    executed_commission: Decimal
    lots_executed: int
    direction: OrderDirection


@dataclass
class PostOrderResponse(ABC):
    execution_report_status: OrderExecutionReportStatus
    order_id: str
    initial_order_price: Decimal


@dataclass
class CancelOrderResponse(ABC):
    pass


@dataclass
class GetOrdersResponse(ABC):
    orders: list[OrderState]


# orders-stream service


class OrdersStreamService(ABC):
    @abstractmethod
    def trades_stream(self, *args, **kwargs):
        pass

    @abstractmethod
    def order_state_stream(self, *args, **kwargs):
        pass


# users service


class UsersService(ABC):
    @abstractmethod
    def get_accounts(self, *args, **kwargs):
        pass


@dataclass
class Account(ABC):
    id: str


# sandbox service


class SandboxService(ABC):
    @abstractmethod
    def open_sandbox_account(self, *args, **kwargs):
        pass

    @abstractmethod
    def sandbox_pay_in(self, *args, **kwargs):
        pass

    @abstractmethod
    def close_sandbox_account(self, *args, **kwargs):
        pass
