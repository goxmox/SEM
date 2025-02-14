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

    def get_feature_names_out(self, input_features=None):
        return self.feature_names_out_

    def fit(self, X, y=None, **kwargs):
        self.last_candle: pd.DataFrame = None

        return self

    def transform(self, X):
        if isinstance(X, list):
            if isinstance(X[0], pd.DataFrame):
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
        return self

    def load_model(self, data):
        self.last_candle = data['last_candle']


class CandlesToDirection(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            bull_threshold=8,
            bear_threshold=-8,
            periods=5
    ):
        self.bull_threshold = bull_threshold
        self.bear_threshold = bear_threshold
        self.periods = periods

        self.last_candles = pd.DataFrame([])

    def fit(self, X, y=None):
        self.last_candles = pd.DataFrame([])

        return self

    def transform(self, X):
        if isinstance(X, list):
            if isinstance(X[0], pd.DataFrame):
                X = pd.concat(X)

        if len(self.last_candles) > 0:
            X = pd.concat([self.last_candles, X])

        t_index = X.index

        directions_bull = np.log(X['high'].iloc[self.periods - 1:]) - np.log(X['open'].iloc[:X.shape[0] - self.periods + 1])
        directions_bear = np.log(X['low'].iloc[self.periods - 1:]) - np.log(X['open'].iloc[:X.shape[0] - self.periods + 1])