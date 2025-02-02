from enum import Enum


class SessionAuction(Enum):
    CLOSED = 0
    TWOSIDED = 1
    OPENING = 2
    CLOSING = 3


class InstrumentType(Enum):
    STOCK = 0
    FUTURES = 1


class AccountType(Enum):
    ACCOUNT_TYPE_UNSPECIFIED = 0
    ACCOUNT_TYPE_TINKOFF = 1
    ACCOUNT_TYPE_TINKOFF_IIS = 2
    ACCOUNT_TYPE_INVEST_BOX = 3
    ACCOUNT_TYPE_INVEST_FUND = 4
