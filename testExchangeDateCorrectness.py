from api.tinvest.constants import working_hours, break_in_working_hours, holidays
from engine.schemas.constants import candle_path
import pandas as pd
from datetime import datetime, timezone

if __name__ == '__main__':
    stock_name = 'MGNT'
    type_instrument = 'shares'

    stock = pd.read_csv(candle_path + f'\\{stock_name}\\{stock_name}.csv')
    stock['time'] = pd.to_datetime(stock['time'], format='%Y-%m-%d %H:%M:%S%z')

    for date in stock['time']:
        wh, on_break = False, False

        if date.weekday() >= 5 or date.date() in holidays:
            continue

        working_hours_data = working_hours.fetch_info(type_instrument, date.date())
        start_date = datetime.combine(date.date(), working_hours_data['start'], tzinfo=timezone.utc)
        end_date = start_date + working_hours_data['duration']

        if not (start_date <= date < end_date):
            wh = True

        break_data = break_in_working_hours.fetch_info(type_instrument, date.date())

        for break_ in break_data:
            start_date = datetime.combine(date.date(), break_['start'], tzinfo=timezone.utc)
            end_date = start_date + break_['duration']

            if start_date <= date < end_date:
                on_break = True

        if wh:
            print(f'{date.date()} has wrong working hours for futures')
            break
        if on_break:
            print(f'{date} has wrong break hours for futures')
