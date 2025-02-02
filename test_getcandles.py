from time import time
from api.tinvest.set_token import set_token
from api.tinvest.tclient import TClient
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.candles.candles_uploader import LocalCandlesUploader
from engine.schemas.data_broker import DataTransformerBroker
LocalCandlesUploader.broker = t_invest

if __name__ == '__main__':
    ticker = TTicker('MGNT')
    set_token()

   # candles = DataTransformerBroker(TTicker('MGNT')).make_pipeline([]).compute()

    with TClient(sandbox=False, trade=False) as client:
        client.services.get_candles(ticker)
        LocalCandlesUploader.cache_new_candles()

    #print(time() - start)
