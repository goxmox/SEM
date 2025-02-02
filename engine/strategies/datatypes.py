from dataclasses import dataclass, field
from engine.schemas.enums import OrderDirection, OrderType, OrderExecutionReportStatus
from engine.schemas.datatypes import Ticker
from engine.schemas.client import Client, Account


@dataclass
class OrderManager:
    client: Client
    account: Account
    tickers_collection: list[Ticker]
    orders: list['LocalOrder'] = field(default_factory=list)
    relevant_orders: dict[str, 'LocalOrder'] = field(default_factory=dict)
    new_orders: list['LocalOrder'] = field(default_factory=list)
    transactions: dict[str, dict] = field(default_factory=dict)

    def add_new_orders(
            self,
            orders: list['LocalOrder'],
    ):
        for order in orders:
            order.price = self.client.price_correction(order.price, order.ticker)
            order.quantity = self.client.lots_correction(order.quantity, order.ticker)
            order.order_name += f'_{order.ticker.ticker_sign}'

            self.orders.append(order)
            self.relevant_orders[order.order_name] = order
            self.new_orders.append(order)

    def extract_new_orders(
            self,
    ) -> list['LocalOrder']:
        new_orders = self.new_orders
        self.new_orders = []

        return new_orders

    def select_relevant_order_names(
            self,
            tickers: list[Ticker] = None,
            subname: str = None,
    ) -> list['LocalOrder']:
        rel_order_names = []

        for name in self.relevant_orders.keys():
            if tickers is None:
                if subname is None or subname in name:
                    rel_order_names.append(name)
            else:
                for ticker in tickers:
                    if (subname is None or subname in name) and (ticker.ticker_sign in name):
                        rel_order_names.append(name)

        return rel_order_names

    def select_relevant_orders(
            self,
            tickers: list[Ticker] = None,
            subname: str = None,
    ) -> list['LocalOrder']:
        rel_order_names = self.select_relevant_order_names(tickers, subname)

        return [self.relevant_orders[name] for name in rel_order_names]

    def delete_relevant_orders(
            self,
            tickers: list[Ticker] = None,
            subname: str = None,
    ):
        rel_order_names = self.select_relevant_order_names(tickers, subname)

        for name in rel_order_names:
            order = self.relevant_orders[name]

            if order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
                self.client.services.orders.cancel_order(
                    account_id=self.account.id,
                    order_id=order.order_id
                )

            del self.relevant_orders[name]

    def cancel_relevant_orders(
            self,
            tickers: list[Ticker] = None,
            subname: str = None,
    ):
        rel_orders = self.select_relevant_orders(tickers, subname)

        for order in rel_orders:
            if order.status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
                continue

            self.client.services.orders.cancel_order(
                account_id=self.account.id,
                order_id=order.order_id
            )

            order.status = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_CANCELLED

    def update_relevant_orders(self) -> list['LocalOrder']:
        filled_orders = []

        for order in self.select_relevant_orders():
            if order.status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
                continue

            order_state = self.client.services.orders.get_order_state(
                account_id=self.account.id,
                order_id=order.order_id
            )
            order.status = order_state.execution_report_status

            if (order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
                    or order.status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_PARTIALLYFILL):
                self.record_transaction(order)
                filled_orders.append(order)

        return filled_orders

    def record_transaction(self, order: 'LocalOrder'):
        direction = 1 if order.direction == OrderDirection.ORDER_DIRECTION_BUY else -1

        self.transactions |= {order.order_id:
                                   {'price': direction * order.price,
                                    'quantity': order.quantity,
                                    'commission': order.commission,
                                    'total': direction * order.price * order.quantity
                                    }}


@dataclass
class LocalOrder:
    order_name: str
    order_id: str
    price: float
    quantity: int
    direction: OrderDirection
    instrument_uid: str
    ticker: Ticker
    order_type: OrderType
    account_id: str
    status: OrderExecutionReportStatus = OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_UNSPECIFIED
    commission: float = 0
