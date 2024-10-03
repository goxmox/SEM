from tinkoff.invest import Client
from web import get_token, process_nano, instrumentPath
import pandas as pd
from dataclasses import asdict


def save_instruments(client, instrument_type='shares'):
    if instrument_type == 'shares':
        unitnano_fields = ['min_price_increment', 'nominal', 'dshort_min', 'dlong_min',
                           'dshort', 'dlong', 'kshort', 'klong']
        instruments_data = client.instruments.shares().instruments
    elif instrument_type == 'futures':
        unitnano_fields = ['min_price_increment', 'basic_asset_size', 'dshort_min', 'dlong_min',
                           'dshort', 'dlong', 'kshort', 'klong', 'initial_margin_on_buy',
                           'initial_margin_on_sell', 'min_price_increment_amount']
        instruments_data = client.instruments.futures().instruments
    else:
        raise ValueError('Wrong instrument type')

    instruments_rus = None

    for instrument in instruments_data:
        # leaving only russian stocks which are tradable via this api
        if (instrument.api_trade_available_flag and
                instrument.country_of_risk == 'RU' and '@' not in instrument.ticker and instrument.currency == 'rub'):
            # converting from dataclass to a workable dict
            instrument = asdict(instrument)

            del instrument['brand']

            # these fields are themselves dicts, cause trouble converting to dataframe
            for field in unitnano_fields:
                instrument[field] = process_nano(instrument[field])
            # making this dict convertable to a dataframe
            instrument = {field: [value] for field, value in zip(instrument.keys(), instrument.values())}

            if instruments_rus is None:
                instruments_rus = pd.DataFrame.from_dict(instrument)
            else:
                instruments_rus = pd.concat([instruments_rus, pd.DataFrame(instrument)])

    instruments_rus.to_csv(instrumentPath + f'{instrument_type}.csv', encoding='utf-8-sig', index=False)


token = get_token(False)

with Client(token) as client:
    save_instruments(client, instrument_type='shares')
    save_instruments(client, instrument_type='futures')

