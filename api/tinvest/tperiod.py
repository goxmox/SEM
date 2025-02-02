from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from api.tinvest.utils import infer_start_and_end_date
from api.tinvest.constants import working_hours, break_in_working_hours, working_weekends, session_type
from api.tinvest.datatypes import SessionAuction, InstrumentType
from engine.schemas.enums import SessionPeriod
from engine.schemas.datatypes import Period


@dataclass
class TPeriod(Period):
    time_period: datetime = datetime.now(tz=timezone.utc)
    time_frequency: timedelta = timedelta(minutes=1)
    instrument_session: dict[InstrumentType, SessionPeriod] = None
    instrument_auction: dict[InstrumentType, SessionAuction] = None
    exchange_closed: bool = False
    on_break: bool = False

    def __post_init__(self):
        if self.time_period.tzinfo is None or self.time_period.tzinfo.utcoffset(self.time_period) is None:
            self.time_period = self.time_period.replace(tzinfo=timezone.utc)

        self.instrument_session = {}
        self.instrument_auction = {}
        self._time_from_opening = timedelta(minutes=0)
        self._time_before_closing = timedelta(minutes=0)
        self.update_market_schedule_info()

    def update_market_schedule_info(self):
        if self.time_period.weekday() >= 5 and self.time_period.date() not in working_weekends:
            self.instrument_session = {type_instrument: SessionPeriod.CLOSED for type_instrument in InstrumentType}
            self.instrument_auction = {type_instrument: SessionAuction.CLOSED for type_instrument in InstrumentType}
        else:
            for type_instrument in InstrumentType:
                # checking whether the exchange is open
                if not working_hours.is_datetime_in_relevant_interval(type_instrument, self.time_period):
                    self.exchange_closed = True

                # checking whether there is a break in trading the instrument
                if break_in_working_hours.is_datetime_in_relevant_interval(type_instrument, self.time_period):
                    self.on_break = True

                if self.on_break or self.exchange_closed:
                    self.instrument_session[type_instrument] = SessionPeriod.CLOSED
                    self.instrument_auction[type_instrument] = SessionAuction.CLOSED
                else:
                    self.instrument_session[type_instrument], session_metainfo = (
                        session_type.items_of_relevant_interval(type_instrument, self.time_period))

                    session_start_date, session_end_date = infer_start_and_end_date(self.time_period, session_metainfo)

                    if session_metainfo['opening'] and session_start_date == self.time_period:
                        self.instrument_auction[type_instrument] = SessionAuction.OPENING
                    elif session_metainfo['closing'] and session_end_date == self.time_period:
                        self.instrument_auction[type_instrument] = SessionAuction.CLOSING
                    else:
                        self.instrument_auction[type_instrument] = SessionAuction.TWOSIDED

    def next_period(self, update_with_cur_time):
        if not update_with_cur_time:
            self.time_period += self.time_frequency
        else:
            self.time_period = datetime.now(tz=timezone.utc)

        self.update_market_schedule_info()
