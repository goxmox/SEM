import numpy as np
import pandas as pd
from api.tinvest.utils import get_info_of_instruments
from engine.schemas.constants import candle_path
from api.tinvest.datatypes import InstrumentType
from api.broker_list import t_invest
from engine.schemas.datatypes import Ticker
from typing import Union
import os


class TTicker(Ticker):
    def __init__(self, ticker: str):
        super().__init__()

        self.ticker_sign = ticker

        for type_instrument in InstrumentType:
            info = get_info_of_instruments(type_instrument, broker=t_invest).set_index('ticker')

            if self.ticker_sign in info.index:
                self.type_instrument = type_instrument
                break
        else:
            raise ValueError(f'Ticker {self.ticker_sign} is not in the list of instruments.')

        info = info.loc[self.ticker_sign]
        self.uid = info['uid']
        self.lot = info['lot']
        self.min_price_increment = info['min_price_increment']
        self.klong = info['klong']
        self.short_enabled = info['short_enabled_flag']
