import numpy as np
from engine.schemas.enums import SessionPeriod, OrderDirection, OrderExecutionReportStatus, OrderType
from engine.strategies.strategy import Strategy
from engine.strategies.datatypes import LocalOrder
from engine.schemas.datatypes import Ticker


class TradeAndHold(Strategy):
    def __init__(
            self, tickers: list[Ticker], cash_share: float = 1,
            buy: bool = 1, return_threshold_up=10, return_threshold_down=10, total_no_of_tries_to_execute_initial=3,
            perpetual=False, sessions=(SessionPeriod.MAIN,), **additional_open_to_trading_parameters):
        super().__init__(tickers, cash_share=cash_share, sessions=sessions, **additional_open_to_trading_parameters)

        self._buy = buy
        self._return_threshold_up = return_threshold_up
        self._return_threshold_down = return_threshold_down
        self._executed, self._initial_filled, self._undesired_met = False, False, False
        self._total_no_of_tries_to_execute_initial = total_no_of_tries_to_execute_initial
        self._initial_execution_no_of_tries = 0
        self._perpetual = perpetual

    def _restart(self):
        self.active, self._executed, self._initial_filled, self._undesired_met = True, False, False, False
        self._initial_execution_no_of_tries = 0
        self.ongoing_trading = False
        self._orders, self._transactions = {}, {}
        self._cash = self._client.get_available_balance(self._account) * self._cash_share

    def _set_tickers_for_portfolio_computation(self, new_data=None):
        return self.tickers_collection

    def _determine_signs(self, new_data=None):
        return np.array([self._buy + (-1) * (1 - self._buy)])

    def _determine_lots(self, signed_portfolio, new_data=1):
        return signed_portfolio * (self._cash // self.portfolio_prices[0])

    def _set_portfolio_prices(self, prices_data):
        if self._buy:
            return prices_data[self.tickers_collection[0].uid]['to_buy']
        else:
            return prices_data[self.tickers_collection[0].uid]['to_sell']

    def _update(self):
        # ----- initialization ----------

        if not self._executed:
            self._compute_portfolio()

        direction = OrderDirection.ORDER_DIRECTION_BUY if self._buy == 1 \
            else OrderDirection.ORDER_DIRECTION_SELL
        opposite_direction = OrderDirection.ORDER_DIRECTION_SELL if self._buy == 1 \
            else OrderDirection.ORDER_DIRECTION_BUY
        desired_price = (self.portfolio_prices[0]
                         * (1 + self._return_threshold_up / 10000) if self._buy == 1
                         else self.portfolio_prices[0]
                              * (1 - self._return_threshold_down / 10000))
        undesired_price = (self.portfolio_prices[0]
                           * (1 + self._return_threshold_up / 10000) if self._buy == 0
                           else self.portfolio_prices[0]
                                * (1 - self._return_threshold_down / 10000))

        initial_desired_orders = [
            LocalOrder(
                order_name='initial',
                price=self.portfolio_prices[0],
                lots=self.portfolio_lots[0],
                direction=direction,
                instrument_uid=self.tickers[0].uid,
                order_type=OrderType.ORDER_TYPE_MARKET,
                account_id=self._account.id,
                order_id=''
            ),
            LocalOrder(
                order_name='desired',
                price=desired_price,
                lots=self.portfolio_lots[0],
                direction=opposite_direction,
                instrument_uid=self.tickers[0].uid,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                account_id=self._account.id,
                order_id=''
            )
        ]
        undesired_order = [
            LocalOrder(
                order_name='undesired',
                price=undesired_price,
                lots=self.portfolio_lots[0],
                direction=opposite_direction,
                instrument_uid=self.tickers[0].uid,
                order_type=OrderType.ORDER_TYPE_MARKET,
                account_id=self._account.id,
                order_id=''
            ),
        ]

        # -------- logic ---------------

        if not self._executed:
            self._prepare_order(initial_desired_orders)
            self._trade()

            self._executed = True

        if not self._initial_filled:
            if (self._update_relevant_order_state(self._orders['initial']) ==
                    OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL):
                self._initial_filled = True
            else:
                self._initial_execution_no_of_tries += 1

            if self._initial_execution_no_of_tries == self._total_no_of_tries_to_execute_initial:
                self.terminate()
                if self._perpetual:
                    self._restart()

                return

        if self._initial_filled and self.active:
            if (self._update_relevant_order_state(self._orders['desired']) ==
                    OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL):
                self.terminate()

                if self._perpetual:
                    self._restart()

                return
            else:
                if not self._undesired_met:
                    current_prices = self._get_new_prices()

                    if ((self._buy and current_prices['to_sell'][0] <= undesired_price)
                            or (not self._buy and current_prices['to_buy'][0] >= undesired_price)):
                        self._undesired_met = True

                        self._prepare_order(undesired_order)
                        self._trade()
                else:
                    if (self._update_relevant_order_state(self._orders['undesired']) ==
                            OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL):
                        self.terminate()
                        if self._perpetual:
                            self._restart()

                        return
        else:
            if not self.active:
                raise NotImplementedError('The strategy is no longer active. Execute a new one.')

    def _update_offhours(self):
        return