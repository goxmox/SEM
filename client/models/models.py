from abc import ABC, abstractmethod
from dataclasses import dataclass
import numpy as np
import pandas as pd

from client import Ticker


class Model(ABC):
    @abstractmethod
    def fit(self, inputDf):
        pass

    @abstractmethod
    def predict(self, inputDf):
        pass

    @abstractmethod
    def computePredicts(self, inputDf, testNumDays=44, recursive=True):
        pass

    @property
    @abstractmethod
    def requiredInputs(self) -> list[str]:
        pass


def getInputDf(tickers: list[str], models: list[Model]) -> list[pd.DataFrame]:
    # returns the list of pd.DataFrame with dims "time x rms", where each pd.DataFrame corresponds to a ticker

    namesOfInputs = set()
    for model in models:
        namesOfInputs |= set(model.requiredInputs)
    namesOfInputs = list(namesOfInputs)

    inputsDf = []

    for ticker in tickers:
        inputsDf.append(Ticker(ticker).rmQuery(rms=namesOfInputs))

    return inputsDf


class Models:
    tickers: str | list[str]
    models: Model | list[Model]
    __fitted: bool = 0

    def __init__(self, tickers, models):
        self.tickers = list(tickers)
        self.models = list(models)

    def selectBestModelLoss(self):
        inputsDf = getInputDf(self.tickers, self.models)

        print(inputsDf)

        for model in self.models:
            print(np.mean(model.computeLosses(inputsDf[model.requiredInputs])**2))

    def getTestPredict(self, numDays=44):
        # return the np.array with dims "ticker x model x time" of predicted values of whatever models predict

        inputsDf = getInputDf(self.tickers, self.models)
        testPredict = [[] for idx in range(len(self.tickers))]

        for idx, ticker in enumerate(self.tickers):
            for model in self.models:
                testPredict[idx].append(model.computePredicts(inputsDf[idx][model.requiredInputs], testNumDays=numDays))

        return np.array(testPredict)



