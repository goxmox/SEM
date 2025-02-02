import numpy as np
import pandas as pd
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Union


@dataclass
class L:
    data: pd.Series
    lag: int = 0
    har_length: int = 1


def preprocess_lags(X: list[Union[L, pd.Series]], y: pd.Series):  # assuming y_t = x_{t - x.lag}
    X = [x if type(x) is L else L(x) for x in X]

    min_date = datetime.min.replace(tzinfo=timezone.utc)
    for x in X:
        min_date = max(min_date, x.data.index[0])
    min_date = max(min_date, y.index[0])

    for x in X:
        x.data = x.data[x.data.index >= min_date].to_numpy()

        if x.lag > 0:
            x.data = x.data[:-x.lag]
    y = y[y.index >= min_date].to_numpy()

    max_lag = max([x.lag + x.har_length - 1 for x in X])
    X = np.array([x.data[max_lag - x.lag:] for x in X]).T

    return X, y[max_lag:]
