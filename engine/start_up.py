from engine.schemas.client import Services, Client
from engine.schemas.datatypes import Broker
from engine.schemas.constants import instrument_path
from engine.candles.candles_uploader import LocalCandlesUploader
from api.broker_list import t_invest
from api.tinvest.mock_client import TMockClient
from api.tinvest.tclient import TClient
from api.tinvest.tticker import TTicker
from api.tinvest.set_token import set_token
from typing import Type
import os


def start_up(
        mock=True,
        client_config=None,
        mock_client_config=None,
        set_up_instruments=False
):
    set_token()

    LocalCandlesUploader.broker_name = t_invest.broker_name

    if set_up_instruments:
        with TClient(**client_config) as client:
            client.services.get_instruments()

    if mock:
        return TMockClient, TTicker, mock_client_config
    else:
        return TClient, TTicker, client_config
