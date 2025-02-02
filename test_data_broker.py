from datetime import datetime, timezone, timedelta
import numpy as np
import torch
import matplotlib.pyplot as plt

from pomegranate.distributions import DiracDelta
from pomegranate.gmm import GeneralMixtureModel

from engine.candles.candles_uploader import LocalCandlesUploader
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.schemas.data_broker import DataTransformerBroker
from engine.transformers.returns import Returns
from engine.transformers.realized_measures import RV
from engine.transformers.tech_indicators import RSI, EMATrendIdentifier, EMA
from engine.models.lag import L, preprocess_lags

from engine.models.hmm import HMM
from engine.schemas.data_broker import DataTransformerBroker
from engine.transformers.returns import Returns
from engine.transformers.preprocessing import StandardScaler
from api.tinvest.tticker import TTicker
from api.tinvest.tperiod import TPeriod
from api.broker_list import t_invest
from engine.candles.candles_uploader import LocalCandlesUploader

LocalCandlesUploader.broker = t_invest

# tickers list
tickers = ['SBER', 'VTBR', 'MGNT', 'LKOH', 'MOEX', 'MTSS', 'MVID', 'RUAL', 'TATN', 'YDEX']

LocalCandlesUploader.broker_name = t_invest.broker_name

for ticker in tickers[2:]:
    print(ticker)
    tick = TTicker(ticker)

    for day_ in [1]:
        t = datetime(year=2024, month=12, day=day_).replace(tzinfo=timezone.utc)

        model = HMM(
            normal_states=10,
            zero_states=0,
            verbose=True,
            tol=3000,
            inertia=0.9,
            abs_tol=True,
            num_of_improvements=3,
        )

        pipe = DataTransformerBroker(tick, remove_session=['premarket']).make_pipeline(
            [Returns(keep_overnight=False), StandardScaler(with_mean=False), model],
            end_date=t
        )

        pipe.fit()

        pipe.save_model()

