from engine.schemas.datatypes import Period, Broker
from engine.strategies.strategy import Strategy
from engine.schemas.client import Client
from engine.schemas.enums import AccountType
from datetime import date, timedelta
from time import sleep, time
from typing import Type


class TradingInterface:
    def __init__(
            self,
            account: AccountType,
            strategies: list[Strategy],
    ):
        self._strategies = strategies
        self._account = account

    def launch(
            self,
            client_constructor: Type[Client],
            client_config: dict
    ):
        with client_constructor(**client_config) as client:
            number_of_inactive_strategies = 0

            while len(self._strategies) > number_of_inactive_strategies:
                print(client.period.time_period)
                start = time()

                for strategy in self._strategies:
                    if not strategy.active:
                        continue

                    try:
                        strategy.execute(client, self._account)
                    except Exception as e:
                        print(e)
                        strategy.terminate()

                    print(strategy._orders)

                    if not strategy.active:
                        number_of_inactive_strategies += 1

                sleep(max(client.period_duration - (time() - start), 0))

                try:
                    client.next_period()
                except StopIteration:
                    for strategy in self._strategies:
                        if strategy.active:
                            strategy.terminate()
                            number_of_inactive_strategies += 1
