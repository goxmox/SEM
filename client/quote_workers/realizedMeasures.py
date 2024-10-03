import pandas as pd
import numpy as np


def rv(returns: np.ndarray):
    return np.sum(returns**2)


realizedMeasures = {'rv': rv}