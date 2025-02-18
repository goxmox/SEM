from datetime import datetime, timezone, timedelta
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.candles.candles_uploader import LocalCandlesUploader
from engine.schemas.constants import model_path

from main import main
from engine.strategies.state_based import AvgState

from engine.schemas.data_broker import Pipeline

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
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
            C=1,
            verbose=1
        )

        model = TargetProcessorClassifier(
            target_name=f'direction_{waiting_period}',
            estimator=estimator,
            max_lag_columns={'returns': lags + waiting_period},
            min_lag_columns={'returns': waiting_period},
            remainder='drop',
            classes_name={0: 'calm', 1: 'bull', 2: 'bear'}
        )

        pipe2 = Pipeline(TTicker("SBER")).make_pipeline(
            [RemoveZeroActivityCandles(),
             CandlesToDirection(periods=waiting_period)
             ],
            end_date=train_date
        )

        pipe1 = Pipeline(TTicker("SBER")).make_pipeline(
            [RemoveZeroActivityCandles(),
             Returns(keep_overnight=False, day_number=False, candle_to_price='close', keep_vol=False),
             StandardScaler(with_mean=False),
             model
             ],
            end_date=train_date
        )

        pipe1.union(pipe2)

        fitted_model = pipe1.fit()

        fitted_model.save_model(path_to_model)

        print(fitted_model.model.score(fitted_model.compute(end_date=train_date)))
        print(fitted_model.model.score(fitted_model.compute(
            fit_date=train_date,
            end_date=train_date + timedelta(hours=24)
        )))

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

