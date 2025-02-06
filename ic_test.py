from datetime import datetime, timezone, timedelta

import numpy as np

from engine.models.hmm import HMMLearn
from engine.schemas.data_broker import DataTransformerBroker
from engine.transformers.returns import Returns
from engine.transformers.preprocessing import StandardScaler
from engine.transformers.candles_processing import RemoveZeroActivityCandles
from api.tinvest.tticker import TTicker
from api.tinvest.tperiod import TPeriod
from api.broker_list import t_invest
from engine.candles.candles_uploader import LocalCandlesUploader

import sys
import os

LocalCandlesUploader.broker = t_invest

# tickers list
tickers = ['SBER', 'VTBR', 'MGNT', 'LKOH', 'MOEX', 'MTSS', 'MVID', 'RUAL', 'TATN', 'YDEX']

LocalCandlesUploader.broker_name = t_invest.broker_name


tick = TTicker(tickers[0])
print(tick)

for day_ in [1]:
    #t = datetime(year=2024, month=12, day=day_).replace(tzinfo=timezone.utc)

    model = HMMLearn(
        n_components=int(sys.argv[1]),
        covariance_type='diag',
        verbose=True,
        tol=500,
        n_iter=1000,
        score='state_share'
    )

    pipe = DataTransformerBroker(tick).make_pipeline(
        [
            RemoveZeroActivityCandles(),
            Returns(keep_overnight=False),
            StandardScaler(with_mean=False),
            model
         ],
    #    end_date=t
    )

    pipe.fit(tries=10)
    
    X = pipe.final_datanode.data.to_numpy()
    means = pipe.model.means_ * pipe.final_datanode.transformer.scale_[0] * 10000
    stds = np.sqrt(pipe.model.covars_) * pipe.final_datanode.transformer.scale_[0] * 10000
    
    means = means[:, 0]
    stds = stds[:, 0, 0]

    bic = pipe.model.bic(X)
    aic = pipe.model.aic(X)
    score = pipe.model.score(X)
    ans = f'Number of states={sys.argv[1]}\nAIC: {aic}\nBIC: {bic}\nScore: {score}'

    print(ans)

    with open(os.getcwd() + f'/ans_{sys.argv[1]}_{datetime.now()}.txt', 'w') as log:
        log.write(ans)

