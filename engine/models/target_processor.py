import numpy as np
import pandas as pd
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Union
from sklearn.base import BaseEstimator, RegressorMixin, ClassifierMixin


class TargetProcessor(BaseEstimator):
    def __init__(
            self,
            target_name: Union[str, list[str]],
            estimator,
            target_transformer=None,
            max_lag=0,
            max_lag_columns: dict = None,
            min_lag_columns: dict = None,
            remainder: str = 'drop'
    ):
        if isinstance(target_name, str):
            self.target_name = [target_name]
        else:
            self.target_name = target_name

        self.target_transformer = target_transformer
        if self.target_transformer is not None:
            self.target_transformer.set_output(transform='default')

        self.estimator = estimator

        self.max_lag = max_lag
        self.max_lag_columns = max_lag_columns
        self.min_lag_columns = min_lag_columns
        self.remainder = remainder

        self.last_data = None

    def _transform(self, X):
        lagged_data = []

        if self.max_lag_columns is None:
            for column in X.columns:
                lagged_data.extend([L(X[column], lag=l) for l in range(1, self.max_lag + 1)])
        else:
            if self.min_lag_columns is None:
                self.min_lag_columns = {}

            for column in self.max_lag_columns.keys():
                max_lag = self.max_lag_columns[column]

                if column in self.min_lag_columns.keys():
                    min_lag = self.min_lag_columns[column]
                else:
                    min_lag = 1

                lagged_data.extend([L(X[column], lag=l) for l in range(min_lag, max_lag + 1)])

            if self.remainder == 'passthrough':
                for column in set(X.columns).difference(set(self.max_lag_columns)):
                    lagged_data.append(L(X[column], lag=0))

        X, y = preprocess_lags(
            X=lagged_data,
            y=X[self.target_name]
        )

        if self.target_transformer is not None:
            y = self.target_transformer.fit_transform(y)

        return X, y

    def fit(self, X):
        new_X, y = self._transform(X)
        maximum_lag = X.shape[0] - new_X.shape[0]

        if maximum_lag > 0:
            self.last_data = X.iloc[-maximum_lag:]

        self.estimator.fit(new_X, y)

        return self

    def score(self, X):
        X, y = self._transform(X)

        return self.estimator.score(X, y)

    def predict(self, X):
        X = pd.concat([self.last_data, X])

        self.last_data = X.iloc[-self.last_data.shape[0]:]

        X, _ = self._transform(X)

        return self.estimator.predict(X)


class TargetProcessorClassifier(TargetProcessor, ClassifierMixin):
    def __init__(self, classes_name: dict = None, **kwargs):
        super().__init__(**kwargs)

        self.classes_name = classes_name

    def predict(self, X, save_state=False):
        prediction = super().predict(X)

        return np.array([self.classes_name[pred] for pred in prediction])


@dataclass
class L:
    data: pd.Series
    lag: int = 0


def preprocess_lags(X: list[Union[L, pd.Series]], y: pd.DataFrame = None):  # assuming y_t = x_{t - x.lag}
    X = [x if isinstance(x, L) else L(x) for x in X]

    min_date = datetime.min.replace(tzinfo=timezone.utc)
    for x in X:
        min_date = max(min_date, x.data.index[0])
    min_date = max(min_date, y.index[0])

    for x in X:
        x.data = x.data[x.data.index >= min_date].to_numpy()

        if x.lag > 0:
            x.data = x.data[:-x.lag]
    y = y[y.index >= min_date].to_numpy()

    max_lag = max([x.lag for x in X])
    X = np.array([x.data[max_lag - x.lag:] for x in X]).T

    return X, np.squeeze(y[max_lag:, :])
