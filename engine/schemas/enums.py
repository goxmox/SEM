from enum import IntEnum, Enum
from typing import NewType


class OrderDirection(IntEnum):
    ORDER_DIRECTION_UNSPECIFIED = 0
    ORDER_DIRECTION_BUY = 1
    ORDER_DIRECTION_SELL = 2


class OrderType(IntEnum):
    ORDER_TYPE_UNSPECIFIED = 0
    ORDER_TYPE_LIMIT = 1
    ORDER_TYPE_MARKET = 2
    ORDER_TYPE_BESTPRICE = 3


class OrderExecutionReportStatus(IntEnum):
    EXECUTION_REPORT_STATUS_UNSPECIFIED = 0
    EXECUTION_REPORT_STATUS_FILL = 1
    EXECUTION_REPORT_STATUS_REJECTED = 2
    EXECUTION_REPORT_STATUS_CANCELLED = 3
    EXECUTION_REPORT_STATUS_NEW = 4
    EXECUTION_REPORT_STATUS_PARTIALLYFILL = 5


class SessionPeriod(Enum):
    CLOSED = 0
    PREMARKET = 1
    MAIN = 2
    AFTERHOURS = 3


InstrumentType = NewType('InstrumentType', None)
AccountType = NewType('AccountType', None)
