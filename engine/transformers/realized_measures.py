import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class RV(TransformerMixin, BaseEstimator):
    def __init__(self, keep_timescale=False, keep_vol=False):
        self.keep_timescale=keep_timescale
        self.keep_vol = keep_vol

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame):
        rv = X[['returns']]**2
        rv['day_number'] = X['day_number']
        rv['volume'] = X['volume']

        rv = rv.groupby('day_number').sum()

        if self.keep_timescale:
            pass

        rv.index = pd.to_datetime(np.unique(X.index.date), format="%Y-%m-%d")
        rv.index.name = 'time'
        rv.rename(columns={'returns': 'RV'}, inplace=True)

        return rv
