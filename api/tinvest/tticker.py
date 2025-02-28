from api.tinvest.utils import get_info_of_instruments
from api.tinvest.datatypes import InstrumentType
from api.broker_list import t_invest
from engine.schemas.datatypes import Ticker
from datetime import datetime


class TTicker(Ticker):
    def __init__(self, ticker: str):
        """
        Dataclass containing all necessary ticker information.

        Dataclass containing all necessary ticker information provided from t-api.
        For initialization there must be a record of a ticker in the instrument.csv
        file provided by t-api.
        """

        super().__init__()

        self.ticker_sign = ticker

        for type_instrument in InstrumentType:
            info = get_info_of_instruments(type_instrument, broker=t_invest).set_index('ticker')

            if self.ticker_sign in info.index:
                self.type_instrument = type_instrument

                self.candles_start_date = datetime.strptime(
                    info["first_1min_candle_date"],
                    '%Y-%m-%d %H:%M:%S%z'
                )

                break
        else:
            raise ValueError(f'Ticker {self.ticker_sign} is not in the list of instruments.')

        info = info.loc[self.ticker_sign]
        self.uid = info['uid']
        self.lot = info['lot']
        self.min_price_increment = info['min_price_increment']
        self.klong = info['klong']
        self.short_enabled = info['short_enabled_flag']
