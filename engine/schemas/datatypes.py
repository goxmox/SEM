from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone, date
from dataclasses import dataclass
import os
import pandas as pd
from engine.schemas.enums import InstrumentType
from engine.schemas.constants import candle_path
from decimal import Decimal


@dataclass
class Broker:
    broker_name: str
    break_in_working_hours: 'ExchangeIntervalTree'
    working_hours: 'ExchangeIntervalTree'
    start_date: date
    working_weekends: list[date]
    holidays: list[date]
    session_type: 'ExchangeIntervalTree'
    commission: Decimal


class Period(ABC):
    def __init__(self):
        self.time_period: datetime = datetime.now()
        self.time_frequency: timedelta = timedelta(minutes=1)
        self.exchange_closed: bool = False
        self.on_break: bool = False

    @abstractmethod
    def next_period(self, update_with_cur_time):
        pass


@dataclass
class Ticker(ABC):
    uid: str = None
    ticker_sign: str = None
    min_price_increment: float = None
    lot: int = None
    type_instrument = None

    def __eq__(self, other):
        return (self.uid == other.uid) and (self.ticker_sign == self.ticker_sign)

    def __hash__(self):
        return hash((self.uid, self.ticker_sign))


# ----------------- data structures for schedules of exchanges-related information

def infer_start_and_end_date(date_query, interval_info: dict):
    start_date = datetime.combine(date_query.date(), interval_info['start'], tzinfo=timezone.utc)
    end_date = start_date + interval_info['duration']

    return start_date, end_date


@dataclass
class ExchangeDateTimeInfo:
    time_info: dict[InstrumentType, dict[date, any]]

    def __post_init__(self):
        for type_instrument in self.time_info.keys():
            self.time_info[type_instrument] |= {date.max: []}

    # fetch info at date_key which is greater than a date_query argument
    # implying that the date_key is an effective starting date of new information of the next date_key
    def fetch_info(self, type_instrument: InstrumentType, date_query: date):
        for date_key_l, date_key_r in zip(list(self.time_info[type_instrument].keys())[:-1],
                                          list(self.time_info[type_instrument].keys())[1:]):
            if date_key_l <= date_query < date_key_r:
                if self.time_info[type_instrument][date_key_l] is not None:
                    return self.time_info[type_instrument][date_key_l]

    # provide a list of tuples (date_interval of type [l, r), info_data)
    def fetch_items(self, type_instrument: InstrumentType) -> list[tuple]:
        dates, info = list(self.time_info[type_instrument].keys()), list(self.time_info[type_instrument].values())
        items = [((dates[i], dates[i + 1]), info[i]) for i in range(len(dates) - 1)]

        return items


@dataclass
class ExchangeIntervalTree(ExchangeDateTimeInfo):
    def is_datetime_in_relevant_interval(self, type_instrument: InstrumentType, date_query: datetime):
        ans = False

        intervals = self.fetch_info(type_instrument, date_query.date())
        # if type(intervals) is dict:
        #    raise ValueError('This interval-tree does not support this relevancy check. Try key_of_relevant_interval method.')

        if type(intervals) is not list:
            intervals = [intervals]

        for interval in intervals:
            start_date, end_date = infer_start_and_end_date(date_query, interval)

            ans += start_date <= date_query < end_date

        return ans

    def items_of_relevant_interval(self, type_instrument: InstrumentType, date_query: datetime):
        day_info = self.fetch_info(type_instrument, date_query.date())

        # if type(day_info) is not dict:
        # raise ValueError(
        #    'This interval-tree does not support this relevancy check.'
        #    ' Try is_datetime_in_relevant_interval method.')

        for key, interval_info in day_info.items():
            start_date, end_date = infer_start_and_end_date(date_query, interval_info)

            if start_date <= date_query < end_date:
                return key, interval_info
