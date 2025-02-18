from engine.strategies.strategy import Strategy
from engine.trading_interface import TradingInterface
from engine.start_up import start_up
from datetime import timedelta


def main(
        strategies: list[Strategy] = None,
        client_config: dict = None,
        set_up_instruments: bool = False,
        mock_client_config: dict = None,
        tickers_collection: list[str] = None,
        duration: timedelta = None,
):
    client, tickers_collection, client_config, account = start_up(
        client_config=client_config,
        tickers_collection=tickers_collection,
        mock_client_config=mock_client_config,
        set_up_instruments=set_up_instruments
    )

    if strategies is not None:
        TradingInterface(
            strategies=strategies,
            account=account,
            duration=duration,
        ).launch(
            client_constructor=client,
            client_config=client_config,
            tickers_collection=tickers_collection
        )
