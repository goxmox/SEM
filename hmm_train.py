from datetime import datetime, timezone

from engine.models.hmm import HMMLearn
from engine.schemas.data_broker import Pipeline
from engine.transformers.returns import Returns
from sklearn.preprocessing import StandardScaler
from engine.transformers.candles_processing import RemoveZeroActivityCandles
from api.tinvest.tticker import TTicker
from api.broker_list import t_invest
from engine.candles.candles_uploader import LocalCandlesUploader
from main import main

from engine.strategies.state_based import AvgState

import sys
import os

LocalCandlesUploader.broker = t_invest

from sklearn import set_config
set_config(transform_output="pandas")

argv = True

if argv:
    n_components = int(sys.argv[2])
    n_fits = int(sys.argv[1])
else:
    n_components = 4
    n_fits = 10


# tickers list
tickers = ['SBER', 'VTBR', 'MGNT', 'LKOH', 'MOEX', 'MTSS', 'MVID', 'RUAL', 'TATN', 'YDEX']
tick = TTicker(tickers[0])

# start of the validation set
t = datetime(year=2024, month=12, day=1).replace(tzinfo=timezone.utc)

# model fitting
model = HMMLearn(
    n_components=n_components,
    covariance_type='full',
    verbose=True,
    tol=500,
    n_iter=1000,
)

pipe = Pipeline(tick).make_pipeline(
    [
        RemoveZeroActivityCandles(),
        Returns(keep_overnight=False, day_number=False, candle_to_price='two_way', keep_vol=False),
        StandardScaler(with_mean=False),
        model,
    ],
    end_date=t
)

pipe.fit(tries=n_fits, show_score=True)

X = pipe.final_datanodes.data.to_numpy()
pipe.model.determine_states(X, pipe.fetch_data('Returns').sum(axis=1).to_numpy())

bic = pipe.model.bic(X)
aic = pipe.model.aic(X)
score = pipe.model.score(X)
ans = f'Number of states={n_components}\nAIC: {aic}\nBIC: {bic}\nScore: {score}'

print(ans)

with open(os.getcwd() + f'/ans_{n_components}_{datetime.now()}.txt', 'w') as log:
    log.write(ans)

pipe.save_model()
