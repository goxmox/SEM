from time import time
from api.tinvest.set_token import set_token
from api.tinvest.tclient import TClient
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.candles.candles_uploader import LocalTSUploader
from engine.schemas.pipeline import Pipeline
LocalTSUploader.broker = t_invest

tickers = ['SBER', 'VTBR', 'MGNT', 'LKOH', 'MOEX', 'MTSS', 'MVID', 'RUAL', 'TATN', 'YDEX']

if __name__ == '__main__':
    for tick in tickers[:]:
        ticker = TTicker(tick)
        set_token()

        try:
            candles = Pipeline(ticker).make_pipeline([]).fit()
        except:
            print('no candles yet')
            pass

        with TClient(sandbox=False, trade=False) as client:
            client.services.get_candles(ticker)
            LocalTSUploader.upload_new_observations()

        #print(time() - start)
