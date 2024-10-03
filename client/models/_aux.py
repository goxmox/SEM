import numpy as np


class L(np.ndarray):
    # an object of np.ndarray can be created in three ways:
    # 1) calling the constructor explicitly (i.e. np.array())
    # 2) creating a view (i.e. calling np.array([1,2,3]).view(np.matrix))
    # 3) creating from a template (e.g. taking a slice)
    # np.ndarray does not use __init__, and intializes an object by calling __new__ method.
    # However, __new__ method is called only at the explicit creation of an object (item 1)).
    # In order to preserve an info from 2) and 3), the __array_finalize__ method is called.
    # In the .view case (item 2)), obj argument is the one that is passed to the .view.
    # In the template case (item 3)), obj argument is always of the same type as the self

    def __new__(cls, data, lag=0, harLength=1):
        if harLength < 1:
            raise ValueError('har length should be >= 1')
        elif (harLength - 1) + lag >= len(data):
            raise ValueError('Insufficent amount of data to use the harLength')

        obj = np.asarray([np.mean(data[idx: idx+harLength]) for idx in range(len(data) - lag)]).view(cls)

        obj.lag = getattr(data, 'lag', lag)
        obj.harLength = getattr(data, 'harLength', harLength)

        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        self.lag = getattr(obj, 'lag', 0)
        self.harLength = getattr(obj, 'harLength', 1)


def preprocessLags(*args, y):  # assuming y_t = x_{t - x.lag}
    x = [L(arg) for arg in args]
    maxLag = max([x_.lag + x_.harLength - 1 for x_ in x])
    x = np.array([x_[maxLag - x_.lag:] for x_ in x]).T

    return x, y[maxLag:].reshape(y.shape[0] - maxLag, 1)
