from sklearn.base import TransformerMixin, BaseEstimator
from statsmodels.tsa.api import SimpleExpSmoothing
import pandas as pd


class RSI(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            periods=14,
            ma='expon'
    ):
        self.periods = periods
        self.ma = ma
        self.initial_level: dict = {}

    def fit(self, X):
        if type(X) is list:
            X = pd.concat(X)

        price_diff = X['close'].diff().iloc[1:]

        U, D = price_diff * (price_diff > 0), -1 * price_diff * (price_diff < 0)
        start_SMA_U, start_SMA_D = U.iloc[:self.periods].mean(), U.iloc[:self.periods].mean()

        self.initial_level['U'] = start_SMA_U
        self.initial_level['D'] = start_SMA_D

        return self

    def transform(self, X):
        if type(X) is list:
            if type(X[0]) is pd.DataFrame:
                X = pd.concat(X)

        price_diff = X['close'].diff().iloc[1:]

        U, D = price_diff * (price_diff > 0), -1 * price_diff * (price_diff < 0)

        SEMA_U = SimpleExpSmoothing(U.iloc[self.periods:].to_numpy(), initialization_method='known',
                                    initial_level=self.initial_level['U'])
        SEMA_D = SimpleExpSmoothing(D.iloc[self.periods:].to_numpy(), initialization_method='known',
                                    initial_level=self.initial_level['D'])

        SEMA_U = SEMA_U.fit(smoothing_level=1 / self.periods, optimized=False).fittedfcast
        SEMA_D = SEMA_D.fit(smoothing_level=1 / self.periods, optimized=False).fittedfcast

        RS = SEMA_U / SEMA_D

        RSI = pd.Series(100 - 100 / (1 + RS), name='RSI')
        RSI.index = price_diff.index[self.periods - 1:]

        self.initial_level['U'] = SEMA_U[-1]
        self.initial_level['D'] = SEMA_D[-1]

        return RSI


class EMA(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            periods=14
    ):
        self.periods = periods

    def fit(self, X):
        return self

    def transform(self, X):
        price = X['close']

        EMA = pd.Series(SimpleExpSmoothing(
            price.iloc[self.periods:].to_numpy(),
            initialization_method='known',
            initial_level=price.iloc[:self.periods].mean()
        ).fit().fittedfcast, name='EMA')

        EMA.index = price.index[self.periods - 1:]

        return EMA


class EMATrendIdentifier(TransformerMixin, BaseEstimator):
    def __init__(
            self,
            periods=14
    ):
        self.periods = periods

    def fit(self, X):
        return self

    def transform(self, X):
        price = X['close']

        EMA = pd.Series(SimpleExpSmoothing(
            price.iloc[self.periods:].to_numpy(),
            initialization_method='known',
            initial_level=price.iloc[:self.periods].mean()
        ).fit().fittedvalues, name='EMA')

        EMA.index = price.index[self.periods:]

        return (price.iloc[self.periods:] - EMA) > 0
