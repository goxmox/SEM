from datetime import datetime, timezone, timedelta, date, time
import numpy as np
import pandas as pd
from engine.schemas.datatypes import Ticker, Broker
from engine.schemas.enums import SessionPeriod
from sklearn.base import BaseEstimator, TransformerMixin


def combine_time_timedelta(time_: time, delta: timedelta) -> time:
    return (datetime.combine(date(year=1, month=1, day=1), time_) + delta).time()


# X is expected to be a table of 6 columns: open, high, low, close, volume, time (or date)
class CandlesRefinerTransformer(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            broker: Broker = None,
            ticker: Ticker = None,
            candles_request_date: datetime = datetime.now().astimezone(),
            last_day_number: int = 0
    ):
        self._broker = broker
        self._ticker = ticker
        self._candles_request_date = candles_request_date
        self._last_day_number = last_day_number
        self.feature_names_in_ = ['open', 'close', 'high', 'low', 'volume', 'time']
        self.feature_names_out_ = ['open', 'close', 'high', 'low', 'volume', 'time', 'day_number']

    def fit(self, X: pd.DataFrame, y=None):
        for column in X.columns:
            if column.lower() not in self.feature_names_in_:
                raise ValueError('Column names must be "open", "close", "high", "low", "volume", "time" (or "date")')

        return self

    def get_feature_names_out(self, input_features=None):
        return self.feature_names_out_

    def transform(self, X: pd.DataFrame):
        X = X.set_index('time')

        first_day, first_day_end_idx = self.fill_breaks_in_candle_data_first_day(X)

        if len(X) > first_day_end_idx + 1:
            X = X.iloc[first_day_end_idx + 1:]
        else:
            return first_day

        X = self.clear_redundant_candles(X)

        if len(X) > 0 and len(first_day) > 0:
            return pd.concat([first_day, self.fill_breaks_in_candle_data(data=X)])
        elif len(first_day) > 0:
            return first_day
        elif len(X) > 0:
            return self.fill_breaks_in_candle_data(X)
        else:
            raise ValueError("No candles to enrich.")

    # this function returns filled data time series,
    # indices of the start of trading days (minus the obvious first day index),
    # ASSUMPTION: there is at least one candle between each trading break during each day
    # ASSUMPTION: candle data is: open, low, high, close, volume; with datetime.datetime as index
    def fill_breaks_in_candle_data(
            self,
            data: pd.DataFrame,
    ) -> pd.DataFrame:
        times = data.index.to_series().reset_index(drop=True)
        data = data.to_dict(orient='list')

        timediff = times.diff()
        breaks_duration = timediff.dt.seconds / 60

        # controlling for clearing breaks
        break_in_trade = np.zeros(shape=times.shape)

        for effective_date_interval, break_data in self._broker.break_in_working_hours.fetch_items(
                self._ticker.type_instrument
        ):
            for break_interval in break_data:
                break_interval['end'] = combine_time_timedelta(break_interval['start'], break_interval['duration'])

                clearing_break = ((times.dt.date >= effective_date_interval[0])
                                  & (times.dt.date < effective_date_interval[1])
                                  & (times.dt.time >= break_interval['start']))

                clearing_break = clearing_break.diff() * clearing_break

                breaks_duration = clearing_break * (breaks_duration - break_interval['duration'].seconds / 60) + (
                        1 - clearing_break) * breaks_duration

                break_in_trade += clearing_break

        break_in_trade[0] = False
        break_in_trade = break_in_trade.iloc[1:].reset_index(drop=True)
        break_in_trade[break_in_trade.shape] = False

        times_shift_back1 = times.iloc[:-1]
        times_shift_back1.index += 1
        times_shift_back1[0] = times_shift_back1.iloc[0]

        # controlling for the start of the trading day
        prev_day_schedule = None
        for effective_date_interval, day_schedule in self._broker.working_hours.fetch_items(
                self._ticker.type_instrument
        ):
            trading_day_start = ((times.dt.date >= effective_date_interval[0])
                                 & (times.dt.date < effective_date_interval[1])
                                 & (times.dt.date.diff().dt.days > 0))

            leftmost_trading_day_start = trading_day_start ^ ((times.dt.date > effective_date_interval[0])
                                                              & (times.dt.date < effective_date_interval[1])
                                                              & (times.dt.date.diff().dt.days > 0))

            day_schedule['end'] = combine_time_timedelta(day_schedule['start'], day_schedule['duration'])

            day_schedule_end_hour = (day_schedule['end'].hour * (day_schedule['end'].hour > 0)
                                     + 24 * (day_schedule['end'].hour == 0))

            trading_day_start_durations = ((times.dt.hour - day_schedule['start'].hour) * 60
                                           + times.dt.minute - day_schedule['start'].minute
                                           + (day_schedule_end_hour - times_shift_back1.dt.hour) * 60
                                           + day_schedule['end'].minute - times_shift_back1.dt.minute)

            if prev_day_schedule is not None:
                prev_day_schedule_end_hour = (prev_day_schedule['end'].hour * (prev_day_schedule['end'].hour > 0)
                                              + 24 * (prev_day_schedule['end'].hour == 0))

                trading_day_start_durations = (((prev_day_schedule_end_hour - day_schedule_end_hour) * 60
                                                + prev_day_schedule['end'].minute - day_schedule[
                                                    'end'].minute) * leftmost_trading_day_start
                                               + trading_day_start_durations)

            breaks_duration = (trading_day_start * trading_day_start_durations
                               + (1 - trading_day_start) * breaks_duration)

            prev_day_schedule = day_schedule

        breaks_duration = breaks_duration.iloc[1:].reset_index(drop=True)
        breaks_duration[breaks_duration.shape] = (
                (day_schedule_end_hour - times.iloc[-1].hour) * 60
                + day_schedule['end'].minute - times.iloc[-1].minute)

        trading_day_end = (times.dt.date.diff().dt.days > 0).iloc[1:].reset_index(drop=True)
        trading_day_end[trading_day_end.shape] = True
        breaks_duration.iloc[-1] = 1

        filled_data = {key: [] for key in self.feature_names_out_}
        filled_dates = []

        times_shift_1 = times.iloc[1:].reset_index(drop=True)
        times_shift_1[times_shift_1.shape] = times_shift_1.iloc[0]

        # filling up new vector of data, and vector of indices of trading day ends
        cur_idx_data, clearing_break_number = 0, 0
        day_number = self._last_day_number + 1
        times_iter, times_shift_iter, trading_day_end_iter, break_in_trade_iter = \
            times.items(), times_shift_1.items(), trading_day_end.items(), break_in_trade.items()
        for idx, duration in breaks_duration.items():
            if idx % 10000 == 0:
                print(idx, '/', len(breaks_duration))
            end_of_day, trading_break = next(trading_day_end_iter)[1], next(break_in_trade_iter)[1]
            t, t_next = next(times_iter)[1], next(times_shift_iter)[1]
            prev_day_j, break_j = 0, 0
            fake_break = False
            break_dur = 0

            # we need to differentiate here between the previous day
            # and the next day
            if end_of_day:
                next_day_schedule = self._broker.working_hours.fetch_info(self._ticker.type_instrument, t_next.date())
                next_day_open = datetime.combine(t_next.date(), next_day_schedule['start'], tzinfo=timezone.utc)

                prev_day_schedule = self._broker.working_hours.fetch_info(self._ticker.type_instrument, t.date())
                prev_day_close = datetime.combine(t.date(), prev_day_schedule['start'],
                                                  tzinfo=timezone.utc) + prev_day_schedule['duration']

            # we also need to establish the duration and date of the next break
            if trading_break:
                break_data = self._broker.break_in_working_hours.fetch_info(self._ticker.type_instrument, t.date())

            for j in range(cur_idx_data, cur_idx_data + int(duration)):
                # skipping inserts if end_of_day covers trading breaks too
                # which is captured by nonzero clearing_break_number
                if clearing_break_number < len(break_data) and end_of_day:
                    prev_day_incr = t + timedelta(minutes=j - cur_idx_data)
                    #print(t, prev_day_incr)

                    if (prev_day_incr < prev_day_close
                            and self._broker.break_in_working_hours.is_datetime_in_relevant_interval(
                            self._ticker.type_instrument, prev_day_incr)):
                        fake_break = True
                        continue
                    elif fake_break:
                        clearing_break_number += 1
                        fake_break = False

                # inserting data into the timeseries
                if j == cur_idx_data:
                    for key in ['open', 'high', 'low', 'close', 'volume']:
                        filled_data[key].append(data[key][idx])
                # inserting missing data into the timeseries
                else:
                    for key in ['open', 'high', 'low', 'close']:
                        filled_data[key].append(data['close'][idx])
                    filled_data['volume'].append(0)

                filled_data['day_number'].append(day_number)

                # inserting missing datetime into the timeseries
                if end_of_day:
                    prev_day_incr = t + timedelta(minutes=j - cur_idx_data)

                    if prev_day_incr < prev_day_close:
                        filled_dates.append(prev_day_incr)
                        prev_day_j += 1

                        if prev_day_incr + timedelta(minutes=1) >= prev_day_close:
                            day_number += 1
                            clearing_break_number = 0
                    else:
                        filled_dates.append(
                            next_day_open + timedelta(minutes=j - cur_idx_data - prev_day_j)
                        )
                elif trading_break:
                    break_incr = t + timedelta(minutes=j - cur_idx_data + break_dur)

                    if clearing_break_number < len(break_data):
                        break_time = break_data[clearing_break_number]['start']
                        break_date = datetime.combine(t.date(), break_time, tzinfo=timezone.utc)

                        if break_incr == break_date - timedelta(minutes=1):
                            break_dur += break_data[clearing_break_number]['duration'].seconds // 60
                            clearing_break_number += 1

                    filled_dates.append(break_incr)
                else:
                    filled_dates.append(t + timedelta(minutes=j - cur_idx_data))

            cur_idx_data = cur_idx_data + int(duration)

        filled_data['time'] = filled_dates
        return pd.DataFrame(filled_data).set_index('time')

    def fill_breaks_in_candle_data_first_day(
            self,
            data: pd.DataFrame
    ) -> pd.DataFrame:
        def working_hours(candle_date):
            return (candle_date.date() >= self._broker.start_date
                    and self._broker.working_hours.is_datetime_in_relevant_interval(
                        self._ticker.type_instrument, candle_date)
                    and not self._broker.break_in_working_hours.is_datetime_in_relevant_interval(
                        self._ticker.type_instrument, candle_date)
                    and (candle_date.weekday() < 5 or candle_date.date() in self._broker.working_weekends)
                    and candle_date.date() not in self._broker.holidays
                    )

        first_candle_date: datetime = data.index[0]
        prev_candle_date: datetime = data.index[0]
        refined_first_day_candles = []
        first_day_end_idx = 0

        for cur_candle_date, candle in data.iterrows():
            candle['day_number'] = self._last_day_number

            if (cur_candle_date - first_candle_date).days > 0:
                break
            elif cur_candle_date == first_candle_date:
                if working_hours(first_candle_date):
                    refined_first_day_candles.append(candle)
                prev_candle_date = cur_candle_date
                continue

            first_day_end_idx += 1

            for j in range(1, (cur_candle_date - prev_candle_date).seconds // 60 + 1):
                cur_t = prev_candle_date + timedelta(minutes=j)

                if working_hours(cur_t):
                    if j < (cur_candle_date - prev_candle_date).seconds // 60:
                        new_candle = candle.copy()
                        new_candle['volume'] = 0
                        new_candle['open'] = new_candle['close']
                        new_candle['high'] = new_candle['close']
                        new_candle['low'] = new_candle['close']

                        refined_first_day_candles.append(new_candle)
                    else:
                        refined_first_day_candles.append(candle)

            prev_candle_date = cur_candle_date

        while cur_candle_date <= self._candles_request_date and (cur_candle_date - first_candle_date).days == 0:
            if working_hours(cur_candle_date):
                new_candle = candle.copy()
                new_candle['volume'] = 0
                new_candle['open'] = new_candle['close']
                new_candle['high'] = new_candle['close']
                new_candle['low'] = new_candle['close']

                refined_first_day_candles.append(new_candle)

            cur_candle_date += timedelta(minutes=1)

        if refined_first_day_candles:
            refined_first_day_candles = pd.concat(refined_first_day_candles, axis=1).T
            refined_first_day_candles.index.name = 'time'
        else:
            refined_first_day_candles = pd.DataFrame([])

        return refined_first_day_candles, first_day_end_idx

    def clear_redundant_candles(
            self,
            candles: pd.DataFrame,
    ) -> pd.DataFrame:
        nonredundant_candle = []

        for candle_date in candles.index:
            nonredundant_candle.append(bool(
                candle_date.date() >= self._broker.start_date
                and self._broker.working_hours.is_datetime_in_relevant_interval(
                    self._ticker.type_instrument, candle_date)
                and not self._broker.break_in_working_hours.is_datetime_in_relevant_interval(
                    self._ticker.type_instrument, candle_date)
                and (candle_date.weekday() < 5 or candle_date.date() in self._broker.working_weekends)
                and candle_date.date() not in self._broker.holidays
            ))

        return candles[nonredundant_candle]


class RemoveSession(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            broker: Broker = None,
            ticker: Ticker = None,
            remove_session: list[str] = None
    ):
        session = {
            'premarket': SessionPeriod.PREMARKET,
            'main': SessionPeriod.MAIN,
            'afterhours': SessionPeriod.AFTERHOURS
        }

        self._broker = broker
        self._ticker = ticker
        self.remove_session = [session[s] for s in remove_session]

    def fit(self, X):
        return self

    def transform(self, X: pd.DataFrame):
        sessions_schedule = self._broker.session_type.fetch_items(self._ticker.type_instrument)
        candles_df = []

        for period in sessions_schedule:
            indices_to_retain = X.index > datetime.min.replace(tzinfo=timezone.utc)

            start_period, end_period = period[0]

            for session_to_remove in self.remove_session:
                if session_to_remove in period[1].keys():
                    schedule = period[1][session_to_remove]
                    start_time = schedule['start']
                    end_time = combine_time_timedelta(schedule['start'], schedule['duration'])

                    indices_to_retain *= (X.index.time < start_time) & (X.index.time >= end_time)

            candles_df.append(
                X.loc[
                    (start_period <= X.index.date) & (X.index.date < end_period)
                    & indices_to_retain
                ]
            )

        return pd.concat(candles_df)

class RemoveZeroActivityCandles(TransformerMixin, BaseEstimator):
    def __init__(self):
        self.name = 'RemoveZeroActivityCandles'

    def fit(self, X):
        return self

    def transform(self, X: pd.DataFrame):
        return X[X['volume'] != 0]

    def save_model(self):
        return {}

    def load_model(self, data):
        pass