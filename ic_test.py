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
        verbose=True,
        tol=1000,
    )

    pipe = DataTransformerBroker(tick, remove_session=['premarket']).make_pipeline(
        [
            RemoveZeroActivityCandles(),
            Returns(keep_overnight=False),
            StandardScaler(with_mean=False),
            model
         ],
    #    end_date=t
    )

    pipe.fit()

    bic, aic = pipe.model.bic(), pipe.model.aic()
    ans = f'Number of states={sys.argv[1]}\nAIC: {aic}\nBIC: {bic}'

    with open(os.getcwd() + f'/ans_{sys.argv[1]}.txt', 'w') as log:
        log.write(ans)

