from engine.strategies.strategy import Strategy
from engine.strategies.datatypes import LocalOrder
from engine.schemas.enums import SessionPeriod, OrderType, OrderDirection
from engine.schemas.datatypes import Ticker
from engine.schemas.data_broker import DataTransformerBroker
from engine.transformers.returns import Returns
from pomegranate.distributions import Normal
from pomegranate.gmm import GeneralMixtureModel
from pomegranate.hmm import DenseHMM
import numpy as np


class HMMReturnsTrade(Strategy):
    def __init__(
            self,
            tickers: list[Ticker],
            cash_share: float = 1,
            buy: bool = 1,
            return_threshold_up=10,
            return_threshold_down=10,
            max_num_of_tickers=2,
            num_of_averaging=2,
            num_of_states=4,
            sessions=(SessionPeriod.MAIN,),
            **additional_open_to_trading_parameters
    ):
        super().__init__(
            tickers,
            cash_share=cash_share,
            sessions=sessions,
            **additional_open_to_trading_parameters
        )

        self._buy = buy
        self._return_threshold_up = return_threshold_up
        self._return_threshold_down = return_threshold_down
        self._max_num_of_tickers = max_num_of_tickers
        self._num_of_averaging = num_of_averaging

        self._executed = False
        self._distributions = {ticker.uid: [Normal() for i in range(num_of_states)] for ticker in tickers}
        self._hmm = {}
        self._num_of_states = num_of_states
        self._state_volatility_threshold = 2
        self._ticker_markov_state = {ticker.uid: -1 for ticker in tickers}
        self.batch_size = 256
        self._retrained_at_night = False

        self._num_of_executed_averaging_orders = {ticker.uid: 0 for ticker in tickers}
        self._lots_executed = {ticker.uid: 0 for ticker in tickers}
        self._current_sum_of_market_orders = {ticker.uid: 0 for ticker in tickers}
        self._threshold_price = {ticker.uid: np.inf if buy else 0 for ticker in tickers}
        self._num_of_order = {ticker.uid: 0 for ticker in tickers}

    def _determine_signs(self, new_data=None):
        signs = {}

        for ticker in self._selected_tickers_to_trade:
            signs |= {ticker.uid: self._buy + (-1) * (1 - self._buy)}

        return signs

    def _determine_lots(self, signed_portfolio, new_data=1):
        lots = {}

        rate = -self._return_threshold_down if self._buy == 1 else self._return_threshold_down
        share = (1 / self._max_num_of_tickers
                 * (rate / ((1 + rate) ** self._num_of_averaging - 1)))

        for ticker in self._selected_tickers_to_trade:
            lots = {ticker.uid: signed_portfolio[ticker.uid] * self._cash * share // self.portfolio_prices[ticker.uid]}

        return lots

    def _set_portfolio_prices(self, prices_data):
        prices = {}

        for ticker in self._selected_tickers_to_trade:
            if self._buy:
                prices |= {ticker.uid: prices_data[ticker.uid]['to_buy']}
            else:
                prices |= {ticker.uid: prices_data[ticker.uid]['to_sell']}

        return prices

    def _deselect_ticker(self, ticker: Ticker):
        self._order_manager.delete_relevant_orders(tickers=[ticker])
        self._selected_tickers_to_trade.remove(ticker)
        self._threshold_price[ticker.uid] = np.inf if self._buy else 0
        self._current_sum_of_market_orders[ticker.uid] = 0
        self._num_of_executed_averaging_orders[ticker.uid] = 0
        self._lots_executed[ticker.uid] = 0

    def _update(self):
        # ----- initialization ----------

        if self._retrained_at_night:
            self._retrained_at_night = False

        direction = OrderDirection.ORDER_DIRECTION_BUY if self._buy == 1 \
            else OrderDirection.ORDER_DIRECTION_SELL
        opposite_direction = OrderDirection.ORDER_DIRECTION_SELL if self._buy == 1 \
            else OrderDirection.ORDER_DIRECTION_BUY

        def create_averaging_orders(
                ticker: Ticker,
                current_sum_of_market_orders: float,
                number_of_filled_market_orders: int,
                number_of_order: int
        ):

            current_sum_of_market_orders += (self.portfolio_prices[ticker.uid]
                              * (1 + self._return_threshold_up / 10000) if self._buy == 1
                              else self.portfolio_prices[ticker.uid]
                                   * (1 - self._return_threshold_down / 10000))
            desired_price = current_sum_of_market_orders / (number_of_filled_market_orders + 1)

            return [
                LocalOrder(
                    order_name=f'market_{number_of_order}',
                    price=self.portfolio_prices[ticker.uid],
                    quantity=self.portfolio_lots[ticker.uid],
                    direction=direction,
                    instrument_uid=ticker.uid,
                    ticker=ticker,
                    order_type=OrderType.ORDER_TYPE_MARKET,
                    account_id=self._account.id,
                    order_id=''
                ),
                LocalOrder(
                    order_name=f'desired_{number_of_order}',
                    price=desired_price,
                    quantity=self.portfolio_lots[ticker.uid],
                    direction=opposite_direction,
                    instrument_uid=ticker.uid,
                    ticker=ticker,
                    order_type=OrderType.ORDER_TYPE_LIMIT,
                    account_id=self._account.id,
                    order_id=''
                )
            ]

        def create_unwanted_order(ticker: Ticker, number_of_order: int):
            undesired_price = (self.portfolio_prices[ticker.uid]
                               * (1 + self._return_threshold_up / 10000) if self._buy == 0
                               else self.portfolio_prices[ticker.uid]
                                    * (1 - self._return_threshold_down / 10000))

            return [
                LocalOrder(
                    order_name=f'unwanted_{number_of_order}',
                    price=undesired_price,
                    quantity=self.portfolio_lots[ticker.uid],
                    direction=opposite_direction,
                    instrument_uid=ticker.uid,
                    ticker=ticker,
                    order_type=OrderType.ORDER_TYPE_MARKET,
                    account_id=self._account.id,
                    order_id=''
                )
            ]

        def update_markov_state(ticker: Ticker):
            pass

        # -------- logic ---------------

        if not self._executed:
            for ticker in self._tickers_for_candle_fetching:
                self._services.get_candles(ticker)

            self.return_nodes = {
                ticker.uid: DataTransformerBroker(
                    ticker=ticker).make_pipeline([Returns(keep_overnight=True)]).compute()
                for ticker in self.tickers_collection
            }

            for ticker in self.tickers_collection:
                self.train_model(ticker)

            self._executed = True

        ## updating info on orders

        filled_orders = self._order_manager.update_relevant_orders()

        for order in filled_orders:
            if 'desired' in order.order_name or 'unwanted' in order.order_name:
                self._deselect_ticker(order.ticker)
            if 'market' in order.order_name:
                self._num_of_executed_averaging_orders[order.ticker.uid] += 1
                self._current_sum_of_market_orders[order.ticker.uid] += order.price

        ## updating info on candles

        for ticker in self._tickers_for_candle_fetching:
            if self._services.get_candles(ticker):
                self.return_nodes[ticker.uid].update()

                update_markov_state(ticker)

        ## selecting new tickers for new trade

        for ticker in self._tickers_for_candle_fetching:
            if self._num_of_states - self._ticker_markov_state[ticker.uid] <= self._state_volatility_threshold:
                if (ticker not in self._selected_tickers_to_trade
                        and len(self._selected_tickers_to_trade) < self._max_num_of_tickers):
                    self._selected_tickers_to_trade.add(ticker)

        ## updating prices for selected tickers

        self._compute_portfolio()

        ## creating averaging orders if the threshold is met

        for ticker in self._selected_tickers_to_trade:
            sign = 1 if self._buy else -1

            if sign * self.portfolio_prices[ticker.uid] <= sign * self._threshold_price[ticker.uid]:
                if self._num_of_executed_averaging_orders[ticker.uid] == self._num_of_averaging:
                    self._order_manager.add_new_orders(
                        create_unwanted_order(
                            ticker=ticker,
                            number_of_order=self._num_of_order[ticker.uid]
                        )
                    )
                else:
                    self._order_manager.add_new_orders(
                        create_averaging_orders(
                            ticker=ticker,
                            current_sum_of_market_orders=self._current_sum_of_market_orders[ticker.uid],
                            number_of_filled_market_orders=self._num_of_executed_averaging_orders[ticker.uid],
                            number_of_order=self._num_of_order[ticker.uid]
                        )
                    )

                self._num_of_order[ticker.uid] += 1

                self._order_manager.cancel_relevant_orders(
                    tickers=[ticker],
                    subname='desired'
                )

        ## post new orders

        self._trade()

    def _update_offhours(self):
        if not self._retrained_at_night:
            for ticker in self._tickers_for_candle_fetching:
                self._services.get_candles(ticker)

                self.train_model(ticker)

            self._retrained_at_night = True
