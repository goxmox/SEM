from tinkoff.invest import Quotation
from engine.schemas.constants import instrument_path
from engine.schemas.datatypes import Broker
from api.tinvest.datatypes import InstrumentType
import pandas as pd
import os
from datetime import datetime, timezone
from decimal import Decimal


def infer_start_and_end_date(date_query, interval_info: dict):
    start_date = datetime.combine(date_query.date(), interval_info['start'], tzinfo=timezone.utc)
    end_date = start_date + interval_info['duration']

    return start_date, end_date


def to_quotation(number: Decimal):
    units = int(number)

    decimal_part = int(round(number % 1, 9) * 10**9)

    return Quotation(units=units, nano=decimal_part)


def quotation_to_decimal(number) -> Decimal:
    if type(number) is dict:
        units = str(number['units'])
        nano = str(number['nano'])
    else:
        units = str(number.units)
        nano = str(number.nano)

    if len(nano) < 9:
        nano = '0' * (9 - len(nano)) + nano

    return Decimal(units + '.' + nano)


def quotation_to_float(number) -> float:
    return float(quotation_to_decimal(number))


def get_info_of_instruments(type_instrument: InstrumentType, broker: Broker) -> pd.DataFrame:
    if os.path.isfile(instrument_path + f'{broker.broker_name}\\{type_instrument.name}.csv'):
        return pd.read_csv(instrument_path + f'{broker.broker_name}\\{type_instrument.name}.csv')
    else:
        raise ValueError(f'{type_instrument.name} is not a supported type of instrument.')

