from datetime import datetime, timezone

from engine.models.hmm import HMMLearn
from engine.schemas.data_broker import DataTransformerBroker
from engine.transformers.returns import Returns
from engine.transformers.preprocessing import StandardScaler
from engine.transformers.candles_processing import RemoveZeroActivityCandles
from api.tinvest.tticker import TTicker
from api.broker_list import t_invest
from engine.candles.candles_uploader import LocalCandlesUploader
from main import main

from engine.strategies.state_based import AvgState

import sys
import os

LocalCandlesUploader.broker = t_invest

# tickers list
tickers = ['SBER', 'VTBR', 'MGNT', 'LKOH', 'MOEX', 'MTSS', 'MVID', 'RUAL', 'TATN', 'YDEX']
tick = TTicker(tickers[0])

# start of the validation set
t = datetime(year=2024, month=12, day=1).replace(tzinfo=timezone.utc)

# model fitting
model = HMMLearn(
    n_components=sys.argv[1],
    covariance_type='diag',
    verbose=True,
    tol=500,
    n_iter=1000,
)

pipe = DataTransformerBroker(tick).make_pipeline(
    [
        RemoveZeroActivityCandles(),
        Returns(keep_overnight=False),
        StandardScaler(with_mean=False),
        model
     ],
    end_date=t
)

pipe.fit(tries=3, show_score=True)
print(pipe.fetch_data('Returns').to_numpy())
X = pipe.final_datanode.data.to_numpy()

pipe.model.determine_states(X, pipe.fetch_data('Returns')['returns'].to_numpy())
state_dist = pipe.model.states_map
print(state_dist)
bull_bear = 0

for state in state_dist:
    if state in ['bear', 'bull']:
        bull_bear += 1

bic = pipe.model.bic(X)
aic = pipe.model.aic(X)
score = pipe.model.score(X)
ans = f'Number of states={sys.argv[1]}\nAIC: {aic}\nBIC: {bic}\nScore: {score}\nBear+Bull: {bull_bear}'

print(ans)

with open(os.getcwd() + f'/ans_{2}_{datetime.now()}.txt', 'w') as log:
    log.write(ans)

#pipe.save_model()

# backtest

mock_client_config = {
    'period': t
}

strategy = AvgState(
    [
        RemoveZeroActivityCandles(),
        Returns(keep_overnight=False),
        StandardScaler(with_mean=False),
        model
     ],
)

main(
    strategies=[strategy],
    mock_client_config=mock_client_config,
    tickers_collection=['SBER']
)
