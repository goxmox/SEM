from engine.schemas.datatypes import Period, Broker, Ticker
from engine.strategies.strategy import Strategy
from engine.schemas.client import Client
from engine.schemas.enums import AccountType
from datetime import datetime, timedelta, timezone
from time import sleep, time
from typing import Type


class TradingInterface:
    def __init__(
            self,
            account: AccountType,
            strategies: list[Strategy],
            duration: timedelta = None,
    ):
        self._strategies = strategies
        self._account = account
        self._duration = duration

    def launch(
            self,
            client_constructor: Type[Client],
            client_config: dict,
            tickers_collection: list[Ticker]
    ):
        with client_constructor(**client_config) as client:
            number_of_inactive_strategies = 0
            starting_cash = client._cash

            if self._duration is not None:
                end_date = client.period.time_period + self._duration
            else:
                end_date = datetime.max.replace(tzinfo=timezone.utc)

            while ((len(self._strategies) > number_of_inactive_strategies)
                   and (client.period.time_period < end_date)):
                start = time()

                for strategy in self._strategies:
                    #if (client.period.time_period.hour == 0) and (client.period.time_period.minute == 0):
                    if (client.period.time_period.minute == 0):
                        print(round(100 * sum(strategy.profits) / (starting_cash * strategy._cash_share), 3))

                    if not strategy.active:
                        continue
                    #print(client.period.time_period)
                    strategy.execute(client, self._account, tickers_collection)

                    if not strategy.active:
                        number_of_inactive_strategies += 1

                #sleep(max(client.period_duration - (time() - start), 0))
                #sleep(0.05)

                try:
                    client.next_period()
                except StopIteration:
                    for strategy in self._strategies:
                        if strategy.active:
                            strategy.terminate()
                            number_of_inactive_strategies += 1
