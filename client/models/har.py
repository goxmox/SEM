import numpy as np
import pandas as pd
from numba import njit
from client.models.models import Model
from client.models.aux import preprocessLags, L
from abc import abstractmethod
import statsmodels.api as sm
from dataclasses import dataclass


def calculateLosses(x, y, idxSplit, recursive=True) -> np.array:
    predicts = []
    idxRoll = 0

    for idx in range(idxSplit, x.shape[0]):
        beta = np.linalg.inv(x[idxRoll:idx].T @ x[idxRoll:idx]) @ x[idxRoll:idx].T @ y[idxRoll:idx]
        predict = (x[idx, :] @ beta)[0]

        predicts.append(predict)

        if not recursive:
            idxRoll += 1

    return np.array(predicts)


@dataclass
class HAR(Model):
    __ols: sm.regression.linear_model.RegressionResults = None

    @abstractmethod
    def __prepare_inputs__(self, data, onlyExog=False):
        pass

    def fit(self, data):
        x, y = self.__prepare_inputs__(data)

        self.__ols = sm.OLS(endog=y, exog=sm.add_constant(x)).fit()

        return self

    def predict(self, exog):
        x = sm.add_constant(self.__prepare_inputs__(exog, onlyExog=True))[-1]

        return self.__ols.predict(x)

    def computePredicts(self, data, testNumDays=44, recursive=True) -> np.array:
        x, y = self.__prepare_inputs__(data)

        idxSplit = x.shape[0] - testNumDays
        x = sm.add_constant(x)

        predicts = calculateLosses(x, y, idxSplit, recursive=recursive)

        return predicts


class HAR_RV(HAR):
    requiredInputs = ['rv']

    def __init__(self, transformRV=np.array):
        self.transformRV = transformRV

    def __prepare_inputs__(self, data: pd.DataFrame, onlyExog=False):
        data = self.transformRV(data[HAR_RV.requiredInputs].to_numpy())
        lag = int(not onlyExog)

        x, y = preprocessLags(
            L(data[:, 0], lag=lag, harLength=1), L(data[:, 0], lag=lag, harLength=5), L(data[:, 0], lag=lag, harLength=22),
            y=data[:, 0]
        )

        if onlyExog:
            return x
        else:
            return x, y
