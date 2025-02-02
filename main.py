from api.broker_list import t_invest
from api.tinvest.tperiod import TPeriod
from engine.schemas.enums import SessionPeriod
from engine.strategies.trade_and_hold import TradeAndHold
from engine.trading_interface import TradingInterface
from api.tinvest.datatypes import AccountType
from api.tinvest.set_token import set_token
from datetime import datetime
import pandas as pd
from engine.schemas.constants import log_path
from engine.start_up import start_up

mock = True
tickers = ['SBER']

client_config = {
    'sandbox': True,
    'restart_sandbox_account': False,
    'trade': False,
    'period_duration': 5
}

mock_client_config = {
    'period': datetime(year=2024, month=1, day=10, hour=6, minute=58),
    'tickers': tickers
}

if __name__ == '__main__':
    Client, Ticker, client_config = start_up(
        mock=mock,
        client_config=client_config,
        mock_client_config=mock_client_config
    )

    tickers = [Ticker(ticker) for ticker in tickers]

    strategies = [
        TradeAndHold(
            tickers=tickers,
            return_threshold_up=10,
            return_threshold_down=10,
            buy=True,
            sessions=(
                SessionPeriod.PREMARKET,
                SessionPeriod.MAIN,
                SessionPeriod.AFTERHOURS)
        )
    ]

    TradingInterface(
        strategies=strategies,
        account=AccountType.ACCOUNT_TYPE_TINKOFF,
    ).launch(
        client_constructor=Client,
        client_config=client_config
    )
