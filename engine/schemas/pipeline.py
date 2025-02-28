from engine.schemas.datatypes import Ticker
from engine.candles.candles_uploader import LocalTSUploader
from engine.transformers.candles_processing import RemoveSession
import pandas as pd
from datetime import datetime, timezone
import os
import joblib

from sklearn.pipeline import Pipeline


class TSPipeline(Pipeline):
    def __new__(
            cls,
            *args,
            path: str = None,
            train_split_date: datetime = datetime.now(tz=timezone.utc),
            **kwargs
    ):
        if path is not None:
            pickled_models = []

            for model in os.listdir(path):
                pickled_models.append(path + model)

            latest_model = None

            for path_to_pickled_file in pickled_models:
                fit_date = path_to_pickled_file[path_to_pickled_file.rfind('/') + 1:path_to_pickled_file.rfind('.')]
                fit_date = datetime.strptime(
                    fit_date,
                    '%Y-%m-%d_%H-%M-%S',
                ).replace(tzinfo=timezone.utc)

                if train_split_date >= fit_date:
                    latest_model = path_to_pickled_file
                else:
                    next_cached_model_date = fit_date
                    break
            else:
                next_cached_model_date = datetime.max.replace(tzinfo=timezone.utc)

            pipe = joblib.load(latest_model)
            pipe.next_cached_model_date = next_cached_model_date

            return pipe
        else:
            return super().__new__(cls)

    def __init__(self,
                 steps,
                 fit_date: datetime = datetime.now(tz=timezone.utc),
                 path: str = None,
                 **kwargs,
                 ):
        if path is None:
            self.fit_date: datetime = fit_date
            self.next_cached_model_date: datetime = None
            
        super().__init__(steps, **kwargs)

    # def add_nodes(
    #         self,
    #         steps: list,
    #         add_prefix: str = None,
    # ):
    #     if self.final_datanodes is None:
    #         parent_nodes = [DataNode(ticker=self.ticker, remove_session=self.remove_session)]
    #         self.nodes[self.ticker].extend(parent_nodes)
    #     else:
    #         parent_nodes = self.final_datanodes
    # 
    #     for idx, step in enumerate(steps):
    #         if (idx == len(steps) - 1) and (not hasattr(step, 'transform')):
    #             self.model = step
    #         else:
    #             new_node = DataNode(
    #                 ticker=self.ticker,
    #                 transformer=step,
    #                 parents=parent_nodes
    #             )
    # 
    #             for parent_node in parent_nodes:
    #                 parent_node.children.append(new_node)
    # 
    #             parent_nodes = [new_node]
    # 
    #             if self.data_name == '':
    #                 self.data_name += repr(step)
    #             else:
    #                 self.data_name += '__' + repr(step)
    # 
    #     self.final_datanodes = parent_nodes
    # 
    #     for final_node in self.final_datanodes:
    #         final_node.data_broker.append(self)
    #         final_node.prefix = add_prefix
    # 
    #     self.optimized[self.ticker] = False
    # 
    #     return self

    # def _compute_X(self, X: pd.DataFrame, fit_date: datetime = None, end_date: datetime = None):
    #     if end_date is None:
    #         end_date = self.end_date
    # 
    #     #self._optimize()
    # 
    #     new_data = []
    # 
    #     for final_datanode in self.final_datanodes:
    #         new_data.append(final_datanode.fit(X, fit_date=fit_date, end_date=end_date))
    # 
    #     return pd.concat(new_data, axis=1, join='inner')
    # 
    # def union(self, other_pipe: 'Pipeline'):
    #     self.final_datanodes.extend(other_pipe.final_datanodes)
    # 
    #     return self
    # 
    # def fit(self, X: pd.DataFrame, y=None, tries=1, show_score=False, **kwargs):
    #     data = self._compute_X(X)
    #     self.fit_date = data.index[-1]
    # 
    #     models = [self.model]
    #     if tries > 1:
    #         models += [copy.deepcopy(self.model) for _ in range(tries - 1)]
    # 
    #     scores = []
    # 
    #     for i, model in enumerate(models):
    #         model.fit(data, y=y, **kwargs)
    #         score = model.score(data, **kwargs)
    # 
    #         if show_score:
    #             print(f'[{i}] Score: {score}')
    # 
    #         scores.append(score)
    # 
    #     self.model = models[scores.index(max(scores))]
    # 
    #     return self
    # 
    # def predict(self, X: pd.DataFrame, new_date: datetime):
    #     data = []
    # 
    #     for final_datanode in self.final_datanodes:
    #         new_data = final_datanode.update(X, new_date)
    # 
    #         if len(new_data) > 0:
    #             data.append(new_data)
    # 
    #     if len(data) > 0:
    #         data = pd.concat(data, axis=1, join='inner')
    # 
    #     self.end_date = new_date
    # 
    #     if len(data) > 0:
    #         return self.model.predict(X=data)
    #     else:
    #         return

    def save(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)

        path += f'{self.fit_date.strftime("%Y-%m-%d_%H-%M-%S.pkl")}'

        joblib.dump(self, path)

    def reload_model(self, path, cur_date: datetime):
        if cur_date >= self.next_cached_model_date:
            self.load_model(path)

            return True
        else:
            return False


class DataNode:
    def __init__(
            self,
            ticker: Ticker,
            transformer=None,
            parents: list['DataNode'] = None,
            remove_session: list[str] = None
    ):
        self.ticker = ticker
        self.transformer = transformer
        self.parents = parents
        self.end_date = None
        self.children = []
        self.data = None
        self.new_data = []
        self.n_of_new_data_processed = 0
        self.data_broker = []
        self.remove_session = remove_session

        self.prefix = None

        self.fitted = False

    def fit(self, X: pd.DataFrame, fit_date=None, end_date=None):
        if self.end_date is None:
            self.end_date = end_date

        if self.parents is not None:
            data = []

            for parent in self.parents:
                if parent.data is None:
                    parent_data = parent.fit(X, fit_date=fit_date, end_date=end_date)
                else:
                    parent_data = parent.data

                if len(parent_data) == 0:
                    data.append(pd.DataFrame())
                elif (self.data is None) and (not self.fitted):
                    data.append(self.transformer.fit_transform(parent_data))

                    self.fitted = True
                elif self.data is None:
                    data.append(self.transformer.transform(parent.data))

            if self.data is None:
                self.data = pd.concat(data, axis=1, join='inner')

                if self.prefix is not None:
                    self.data = self.data.add_prefix(self.prefix)
        else:
            if self.data is None:
                self.data = X

                if fit_date is None:
                    self.data = self.data[self.data.index <= end_date]
                else:
                    self.data = self.data[(fit_date < self.data.index) & (self.data.index <= end_date)]

                if self.remove_session is not None:
                    self.data = RemoveSession(
                        broker=LocalTSUploader.broker,
                        ticker=self.ticker,
                        remove_session=self.remove_session
                    ).fit_transform(self.data)

        return self.data

    def update(self, X: pd.DataFrame, new_date: datetime):
        self.end_date = new_date

        if self.parents is None:
            num_of_new_candle_batches = len(LocalTSUploader.new_candles[self.ticker])\
                                        - self.n_of_new_data_processed

            self.n_of_new_data_processed += num_of_new_candle_batches

            if num_of_new_candle_batches > 0:
                new_data = LocalTSUploader.new_candles[self.ticker][-num_of_new_candle_batches:]
                new_data = pd.concat(new_data)

                if self.remove_session:
                    new_data = RemoveSession(
                        broker=LocalTSUploader.broker,
                        ticker=self.ticker,
                        remove_session=self.remove_session
                    ).transform(new_data)

                self.new_data.append(new_data)
            else:
                if len(self.new_data) == 0:
                    new_data = []
                else:
                    new_data = self.new_data[-1]
        else:
            data = []

            for parent in self.parents:
                updated_data = parent.update(new_date)

                if len(updated_data) == 0:
                    return []

                data.append(updated_data)

            new_parent_data = pd.concat(data, axis=1, join='inner')
            new_data = self.transformer.transform(new_parent_data)

            if self.prefix is not None:
                new_data = new_data.add_prefix(self.prefix)

            self.new_data.append(new_data)

        return new_data

    def cache_new_data(self):
        self.data = pd.concat([self.data] + self.new_data)
        self.n_of_new_data_processed = 0
        self.new_data = []

    def drop_data(self):
        self.data = None
        self.new_data = []

        if self.parents is not None:
            for parent in self.parents:
                parent.drop_data()

    def __eq__(self, other: 'DataNode'):
        return ((repr(self.transformer) == repr(other.transformer))
                and (self.parents == other.parents)
                and (self.ticker == other.ticker)
                and (self.end_date == other.end_date)
                )

    def __repr__(self):
        if self.transformer is None:
            return 'Candle'
        else:
            return repr(self.transformer)
