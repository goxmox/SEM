from abc import ABC, abstractmethod
import pandas as pd

class strategy(ABC):
    @abstractmethod
    def fit(self, path):
        pass

    @abstractmethod
    def predict(self):
        pass


class stockTopVolBuy(strategy):
    def fit(self, path):
        shares = pd.read_csv('/Users/s/Desktop/segr/instruments/shares.csv')[['ticker',
                                                                              'figi', 'lot', 'min_price_increment']]

