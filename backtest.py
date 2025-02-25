from datetime import datetime, timezone, timedelta
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.candles.candles_uploader import LocalCandlesUploader
from engine.schemas.constants import model_path

from main import main
from engine.strategies.state_based import AvgState

from engine.schemas.data_broker import Pipeline

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from engine.transformers.candles_processing import RemoveZeroActivityCandles
from engine.transformers.returns import Returns, CandlesToDirection
from engine.models.target_processor import TargetProcessorClassifier

from sklearn import set_config
set_config(transform_output="pandas")

LocalCandlesUploader.broker = t_invest

train_model = False
backtest_model = True


if __name__ == '__main__':
    train_date = datetime(year=2024, month=12, day=2, hour=8, minute=59).replace(tzinfo=timezone.utc)
    tickers_collection = ['SBER']
    ticker = tickers_collection[0]
    path_to_model = model_path + t_invest.broker_name + f'/{ticker}/LogisticReg(return_lag=5)/'

    if train_model:
        lags = 20
        waiting_period = 5

        estimator = LogisticRegression(
            tol=0.001,
            C=0.5,
            verbose=1
        )

        columns = ['returns'] + [f'onehot_direction_{waiting_period}_{i}' for i in range(3)]
        max_lag = {column: lags + waiting_period for column in columns}
        min_lag = {column: waiting_period for column in columns}

        model = TargetProcessorClassifier(
            target_name=f'direction_{waiting_period}',
            estimator=estimator,
            max_lag_columns=max_lag,
            min_lag_columns=min_lag,
            remainder='drop',
            classes_name={0: 'calm', 1: 'bull', 2: 'bear'}
        )

        pipe2 = Pipeline(TTicker("SBER"), end_date=train_date).add_nodes(
            [RemoveZeroActivityCandles(),
             CandlesToDirection(periods=waiting_period)
             ]
        )

        pipe_onehot = Pipeline(TTicker("SBER"), end_date=train_date).add_nodes(
            [RemoveZeroActivityCandles(),
             CandlesToDirection(periods=waiting_period),
             OneHotEncoder(sparse_output=False),
             ],
            add_prefix='onehot_'
        )

        pipe1 = Pipeline(TTicker("SBER"), end_date=train_date).add_nodes(
            [RemoveZeroActivityCandles(),
             Returns(keep_overnight=False, day_number=False, candle_to_price='close', keep_vol=False),
             StandardScaler(with_mean=False),
             model
             ]
        )

        pipe1.union(pipe2)
        pipe1.union(pipe_onehot)

        print(pipe1._compute_X())

        fitted_model = pipe1.fit()

        fitted_model.save_model(path_to_model)

    if backtest_model:
        duration = timedelta(hours=24)

        mock_client_config = {
            'period': train_date + timedelta(minutes=1),
            'bid_orderbook_price': 'open',
            'ask_orderbook_price': 'open',
            'market_order_price': 'open',
            'buy_price_end_period': 'low',
            'sell_price_end_period': 'high',
            'lag_in_cached_candles': 1,
        }

        strategies = [
            AvgState(
                path_to_model=path_to_model,
                cash_share=0.9,
                num_of_averaging=1,
            )
        ]

        #redetermine_states(model)
        #print(model.states_map)

        main(
            strategies=strategies,
            mock_client_config=mock_client_config,
            tickers_collection=tickers_collection,
            duration=duration,
        )

