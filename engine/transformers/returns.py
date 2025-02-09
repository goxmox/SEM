from sklearn.base import BaseEstimator, TransformerMixin
from typing import Callable
import pandas as pd
import numpy as np
from datetime import timedelta


class Returns(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            candle_to_price: str = 'close',
            keep_overnight: bool = False,
            day_number: bool = False,
            keep_vol: bool = True,
    ):
        self.candle_to_price = candle_to_price
        self.feature_names_out_ = ['returns', 'day_number', 'volume']
        self.keep_overnight = keep_overnight
        self.day_number = day_number
        self.keep_vol = keep_vol
        self.last_candle: pd.DataFrame = None
        self.name = 'Returns(' + f'candle_to_price={candle_to_price},keep_overnight={str(keep_overnight)},' \
                    + f'keep_vol={str(keep_vol)}' + ')'

    def get_feature_names_out(self, input_features=None):
        return self.feature_names_out_

    def fit(self, X, y=None, **kwargs):
        return self

    def transform(self, X):
        if type(X) is list:
            if type(X[0]) is pd.DataFrame:
                X = pd.concat(X)

        if self.last_candle is not None:
            X = pd.concat([self.last_candle, X])

        self.last_candle = X[-1:]

        if self.candle_to_price == 'mean':
            price = np.log(((X['high'] + X['low']) / 2).to_frame())
            price['day_start'] = X['day_number']
        elif self.candle_to_price in ['open', 'high', 'low', 'close']:
            price = np.log(X[self.candle_to_price].to_frame())
            price['day_start'] = X['day_number']

        if self.candle_to_price != 'two_way':
            returns = price.diff()
            returns['day_number'] = X['day_number']
            returns['volume'] = X['volume']
            returns.rename(columns={self.candle_to_price: 'returns'}, inplace=True)
        else:
            returns = np.log(X[['high', 'low']])
            returns['day_start'] = X['day_number'].diff()
            returns['day_number'] = X['day_number']
            returns['high'] -= np.log(X['close'].shift(periods=1))
            returns['low'] -= np.log(X['close'].shift(periods=1))
            returns['volume'] = X['volume']

        if not self.keep_overnight:
            returns = returns[returns['day_start'] == 0]
        else:
            returns = returns.iloc[1:]
        del returns['day_start']

        if not self.day_number:
            del returns['day_number']

        if not self.keep_vol:
            del returns['volume']

        return returns

    def save_model(self):
        data = {
            'last_candle': self.last_candle
        }

        return data

    def load_model(self, data):
        self.last_candle = data['last_candle']

    def __eq__(self, other: 'Returns'):
        return type(other) is Returns and self.candle_to_price == other.candle_to_price
