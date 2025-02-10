from engine.schemas.datatypes import Ticker
from engine.candles.candles_uploader import LocalCandlesUploader
from engine.schemas.constants import candle_path
from engine.transformers.candles_processing import RemoveSession
import pandas as pd
from datetime import datetime, timezone
import os
import joblib
import copy


class DataTransformerBroker:
    nodes: dict[Ticker, list['DataNode']] = {}
    optimized: dict[str, bool] = {}

    def __init__(self,
                 ticker: Ticker,
                 remove_session: list[str] = None
                 ):
        self.ticker = ticker
        self.final_datanode: 'DataNode' = None
        self.model = None
        self.data_name = ''
        self.end_date: datetime = None
        self.fit_date: datetime = None
        self.next_cached_model_date: datetime = None
        self.remove_session = remove_session

        if self.ticker not in self.nodes.keys():
            self.nodes[self.ticker] = []

        self.optimized[self.ticker] = False

    def make_pipeline(
            self,
            steps: list,
            end_date: datetime = datetime.now(tz=timezone.utc)
    ):
        parent_node = DataNode(ticker=self.ticker, remove_session=self.remove_session)
        self.nodes[self.ticker].append(parent_node)
        self.end_date = end_date.replace(tzinfo=timezone.utc)

        for idx, step in enumerate(steps):
            if (idx == len(steps) - 1) and ('predict' in dir(step)):
                self.model = step
            else:
                new_node = DataNode(
                    ticker=self.ticker,
                    transformer=step,
                    parent=parent_node
                )

                parent_node.children.append(new_node)
                parent_node = new_node

                if self.data_name == '':
                    self.data_name += step.name
                else:
                    self.data_name += '__' + step.name

        self.final_datanode = parent_node
        self.final_datanode.data_broker.append(self)
        self.optimized[self.ticker] = False

        return self

    def compute(self, fit_date: datetime = None, end_date: datetime = None):
        if end_date is None:
            end_date = self.end_date

        self._optimize()
        self.final_datanode.compute(fit_date=fit_date, end_date=end_date)

        return self.final_datanode.data

    def fetch_data(self, node_subname):
        node = self.final_datanode

        while node is not None:
            if node_subname in node.name:
                return node.data

            node = node.parent

        return

    def fit(self, tries=1, show_score=False, **kwargs):
        data = self.compute()
        self.fit_date = data.index[-1]

        models = [self.model]
        if tries > 1:
            models += [copy.deepcopy(self.model) for _ in range(tries - 1)]

        scores = []

        for i, model in enumerate(models):
            model.fit(data.to_numpy(), **kwargs)
            score = model.score(data.to_numpy(), **kwargs)

            if show_score:
                print(f'[{i}] Score: {score}')

            scores.append(score)

        self.model = models[scores.index(max(scores))]

        return self

    def save_model(self):
        if self.model is None:
            raise ValueError('No model is specified.')

        path = candle_path + LocalCandlesUploader.broker.broker_name

        if not os.path.isdir(path + f'/{self.ticker.ticker_sign}'):
            os.mkdir(path + f'/{self.ticker.ticker_sign}')

        if not os.path.isdir(path + f'/{self.ticker.ticker_sign}/{self.model.name}'):
            os.mkdir(path + f'/{self.ticker.ticker_sign}/{self.model.name}')

        if not os.path.isdir(path + f'/{self.ticker.ticker_sign}/{self.model.name}/{self.data_name}/'):
            os.mkdir(path + f'/{self.ticker.ticker_sign}/{self.model.name}/{self.data_name}/')

        path = (path
                + f'/{self.ticker.ticker_sign}/{self.model.name}/{self.data_name}/'
                + f'{self.fit_date.strftime("%Y-%m-%d_%H-%M-%S.pkl")}')

        data_list = [self.model.save_model()]
        self.final_datanode.save_model(data_list)

        joblib.dump(data_list, path)

    def load_model(self):
        pickled_models = []

        path = candle_path + LocalCandlesUploader.broker.broker_name

        for model in os.listdir(path + f'/{self.ticker.ticker_sign}/{self.model.name}/{self.data_name}/'):
            pickled_models.append(path + f'/{self.ticker.ticker_sign}/{self.model.name}/{self.data_name}/' + model)

        latest_model = None

        for path_to_pickled_file in pickled_models:
            fitted_date = path_to_pickled_file[path_to_pickled_file.rfind('/') + 1:path_to_pickled_file.rfind('.')]
            fitted_date = datetime.strptime(
                fitted_date,
                '%Y-%m-%d_%H-%M-%S',
            ).replace(tzinfo=timezone.utc)

            if self.end_date >= fitted_date:
                latest_model = path_to_pickled_file
                self.fit_date = fitted_date
            else:
                self.next_cached_model_date = fitted_date
                break
        else:
            self.next_cached_model_date = datetime.max.replace(tzinfo=timezone.utc)

        data_list = joblib.load(latest_model)

        if isinstance(data_list[0], type(self.model)):
            self.model = data_list[0]
        else:
            self.model.load_model(data_list[0])

        self.final_datanode.load_model(data_list[1:])

        if self.end_date > self.fit_date:
            new_data = self.compute(fit_date=self.fit_date, end_date=self.end_date)

            if len(new_data) > 0:
                self.model.update(new_data)

        return self

    def reload_model(self, cur_date: datetime):
        if cur_date >= self.next_cached_model_date:
            self.load_model()

            return True
        else:
            return False

    def update(self, new_date: datetime):
        new_data = self.final_datanode.update(new_date)

        if len(new_data) > 0:
            self.model.update(new_data)

        self.end_date = new_date

    def cache_new_data(self):
        self.final_datanode.cache_new_data()

    def _optimize(self):
        if self.optimized[self.ticker]:
            return

        parent_nodes = DataTransformerBroker.nodes[self.ticker]
        children_nodes = []

        while parent_nodes:
            for idx, node in enumerate(parent_nodes):
                children_nodes.extend(node.children)

                for j, same_node in enumerate(parent_nodes[idx + 1:]):
                    if same_node == node:
                        if node.data is None and same_node.data is not None:
                            node.data = same_node.data

                        for child in same_node.children:
                            child.parent = node
                            node.children.append(child)

                        for data_broker in same_node.data_broker:
                            data_broker.final_datanode = node

                        node.data_broker.extend(same_node.data_broker)

                        del parent_nodes[idx + 1 + j]
                        idx -= 1
                        j -= 1

            parent_nodes = children_nodes
            children_nodes = []

        self.optimized[self.ticker] = True


class DataNode:
    def __init__(
            self,
            ticker: Ticker,
            transformer=None,
            parent: 'DataNode' = None,
            remove_session: list[str] = None
    ):
        self.ticker = ticker
        self.transformer = transformer
        self.parent = parent
        self.end_date = None
        self.children = []
        self.data = None
        self.new_data = []
        self.n_of_new_data_processed = 0
        self.data_broker = []
        self.remove_session = remove_session

        self.fitted = False

        if self.transformer is not None:
            self.name = self.transformer.name
        else:
            self.name = 'Candles'

    def compute(self, fit_date=None, end_date=None):
        if self.end_date is None:
            self.end_date = end_date

        if self.parent is not None:
            if self.parent.data is None:
                self.parent.compute(fit_date=fit_date, end_date=end_date)

            if (self.data is None) and (not self.fitted):
                self.data = self.transformer.fit_transform(self.parent.data)
                self.fitted = True
            elif self.data is None:
                if len(self.parent.data) == 0:
                    self.data = pd.DataFrame([])
                else:
                    self.data = self.transformer.transform(self.parent.data)
        else:
            if self.data is None:
                self.data = LocalCandlesUploader.upload_candles(self.ticker)

                if fit_date is None:
                    self.data = self.data[self.data.index <= end_date]
                else:
                    self.data = self.data[(fit_date < self.data.index) & (self.data.index <= end_date)]

                if self.remove_session is not None:
                    self.data = RemoveSession(
                        broker=LocalCandlesUploader.broker,
                        ticker=self.ticker,
                        remove_session=self.remove_session
                    ).fit_transform(self.data)

    def update(self, new_date: datetime):
        self.end_date = new_date

        if self.parent is None:
            num_of_new_candle_batches = len(LocalCandlesUploader.new_candles[self.ticker])\
                                        - self.n_of_new_data_processed

            self.n_of_new_data_processed += num_of_new_candle_batches

            if num_of_new_candle_batches > 0:
                new_data = LocalCandlesUploader.new_candles[self.ticker][-num_of_new_candle_batches:]
            else:
                new_data = []
        else:
            new_parent_data = self.parent.update(new_date)

            if len(new_parent_data) == 0:
                return []

            new_data = self.transformer.transform(new_parent_data)
            self.new_data.append(new_data)

        return new_data

    def cache_new_data(self):
        self.data = pd.concat([self.data] + self.new_data)
        self.n_of_new_data_processed = 0
        self.new_data = []

    def save_model(self, data_list):
        if self.parent is not None:
            data_list.append({'transformer': self.transformer.save_model(), 'fitted': self.fitted})
            self.parent.save_model(data_list)

    def load_model(self, data_list):
        if self.parent is not None:
            if isinstance(data_list[0], dict) and ('fitted' in data_list[0].keys()):
                transformer = data_list[0]['transformer']
                fitted = data_list[0]['fitted']
            else:
                transformer = data_list[0]
                fitted = True

            if isinstance(transformer, type(self.transformer)):
                self.transformer = transformer
            else:
                self.transformer.load_model(transformer)

            self.fitted = fitted

            self.parent.load_model(data_list[1:])

    def __eq__(self, other: 'DataNode'):
        return ((self.transformer == other.transformer)
                and (self.parent == other.parent)
                and (self.ticker == other.ticker)
                and (self.end_date == other.end_date)
                )

    def __repr__(self):
        return self.ticker.ticker_sign + '_' + repr(self.transformer)
