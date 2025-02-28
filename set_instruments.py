from time import time
from api.tinvest.set_token import set_token
from api.tinvest.tclient import TClient
from api.broker_list import t_invest
from api.tinvest.tticker import TTicker
from engine.candles.candles_uploader import LocalTSUploader
from engine.schemas.pipeline import Pipeline
from main import main

if __name__ == '__main__':
    set_token()

    client_config = {
        'sandbox': False,
        'trade': False,
    }

    main(
        client_config = client_config,
        set_up_instruments=True
    )
