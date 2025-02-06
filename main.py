from engine.strategies.strategy import Strategy
from engine.trading_interface import TradingInterface
from engine.start_up import start_up


def main(
        strategies: list[Strategy],
        client_config: dict = None,
        mock_client_config: dict = None,
        tickers_collection: list[str] = None
):
    client, tickers_collection, client_config, account = start_up(
        client_config=client_config,
        tickers_collection=tickers_collection,
        mock_client_config=mock_client_config
    )

    TradingInterface(
        strategies=strategies,
        account=account,
    ).launch(
        client_constructor=client,
        client_config=client_config
    )
