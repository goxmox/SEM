from engine.candles.candles_uploader import LocalCandlesUploader
from api.broker_list import t_invest
from api.tinvest.mock_client import TMockClient
from api.tinvest.tclient import TClient
from api.tinvest.tticker import TTicker
from api.tinvest.datatypes import AccountType


def start_up(
        client_config=None,
        tickers_collection=None,
        mock_client_config=None,
        set_up_instruments=False
):
    mock = (client_config is None) and (mock_client_config is not None)

    if not mock:
        from api.tinvest.set_token import set_token
        set_token()

    LocalCandlesUploader.broker = t_invest

    if set_up_instruments:
        with TClient(**client_config) as client:
            client.services.get_instruments()

    tickers_collection = [TTicker(ticker) for ticker in tickers_collection]

    if mock:
        mock_client_config['tickers'] = tickers_collection

        return TMockClient, tickers_collection, mock_client_config, AccountType.ACCOUNT_TYPE_TINKOFF
    else:
        return TClient, tickers_collection, client_config, AccountType.ACCOUNT_TYPE_TINKOFF
