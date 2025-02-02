import pandas as pd
import sklearn.preprocessing as preprocess


class StandardScaler(preprocess.StandardScaler):
    def __init__(self, copy=True, with_mean=True, with_std=True):
        super().__init__(copy=True, with_mean=with_mean, with_std=with_std)
        self.set_output(transform='pandas')
        self.name = 'StandardScaler'

    def save_model(self):
        data = {
            'scale': self.scale_,
            'mean': self.mean_,
            'n_features_in': self.n_features_in_,
            'feature_names_in': self.feature_names_in_
        }

        return data

    def load_model(self, data):
        self.scale_ = data['scale']
        self.mean_ = data['mean']
        self.var_ = self.scale_ ** 2
        self.n_features_in_ = data['n_features_in']
        self.feature_names_in_ = data['feature_names_in']

