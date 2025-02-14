from torchgen.executorch.api.et_cpp import returns_type

from engine.strategies.strategy import Strategy
from engine.strategies.datatypes import LocalOrder
from engine.schemas.enums import SessionPeriod, OrderType, OrderDirection, OrderExecutionReportStatus
from engine.schemas.datatypes import Ticker
from engine.schemas.data_broker import Pipeline
from engine.transformers.returns import Returns
from pomegranate.distributions import Normal
from pomegranate.gmm import GeneralMixtureModel
from pomegranate.hmm import DenseHMM
import numpy as np


class AvgState(Strategy):
    def __init__(
            self,
            pipeline: list,
            cash_share: float = 1,
            return_threshold_up=10,
            return_threshold_down=10,
            max_num_of_tickers=1,
            num_of_averaging=2,
            states_to_buy=('bull'),
            states_to_sell=('bear'),
            t_threshold=1,
            states_from_train_data=False,
            model_metadata=None,
            sessions=(SessionPeriod.MAIN,),
            **additional_open_to_trading_parameters
    ):
        super().__init__(
            cash_share=cash_share,
            sessions=sessions,
            **additional_open_to_trading_parameters
        )

        self._return_threshold_up = return_threshold_up
        self._return_threshold_down = return_threshold_down
        self._max_num_of_tickers = max_num_of_tickers
        self._num_of_averaging = num_of_averaging

        self._executed = False
        self._pipeline = pipeline
        self._t_threshold = t_threshold
        self._states_from_train_data = states_from_train_data
        self._model_metadata = model_metadata
        self._ticker_pipelines = None

        self._num_of_executed_averaging_orders = None
        self._lots_executed = None
        self._current_sum_of_market_orders = None
        self._threshold_price = None
        self._num_of_order = None
        self._ticker_state: dict = None

        self._states_to_buy = states_to_buy
        self._states_to_sell = states_to_sell

        self.profits = []

    def _determine_lots(self, ticker: Ticker):
        rate = max(self._return_threshold_up, self._return_threshold_down)
        # share = (1 / self._max_num_of_tickers
        #          * (rate / ((1 + rate) ** self._num_of_averaging - 1)))
        share = 1 / self._num_of_averaging

        if ticker in self._selected_tickers_to_sell:
            lots = -1 * self._cash * share // self.portfolio_prices[ticker][
                    OrderDirection.ORDER_DIRECTION_SELL]
        elif ticker in self._selected_tickers_to_buy:
            lots = self._cash * share // self.portfolio_prices[ticker][
                OrderDirection.ORDER_DIRECTION_BUY]

        if lots > 0:
            lots = int(lots // ticker.lot)
        else:
            lots = int(abs(lots + (lots % ticker.lot)) // ticker.lot)

        return lots

    def _set_portfolio_prices(self, prices_data):
        prices = {}

        for ticker in self._selected_tickers_to_trade:
            prices |= {ticker:
                           {OrderDirection.ORDER_DIRECTION_BUY: prices_data[ticker]['to_buy'][0],
                            OrderDirection.ORDER_DIRECTION_SELL: prices_data[ticker]['to_sell'][0]}
                       }

        return prices

    def _deselect_ticker(self, ticker: Ticker):
        self.profits.append(self._order_manager.profit_from_relevant_orders(tickers=[ticker]))

        self._order_manager.delete_relevant_orders(tickers=[ticker])
        self._selected_tickers_to_trade.remove(ticker)

        if ticker in self._selected_tickers_to_buy:
            self._selected_tickers_to_buy.remove(ticker)
        else:
            self._selected_tickers_to_sell.remove(ticker)

        self._threshold_price[ticker] = np.inf
        self._current_sum_of_market_orders[ticker] = 0
        self._num_of_executed_averaging_orders[ticker] = 0
        self._lots_executed[ticker] = 0
        self._ticker_state[ticker] = 'calm'

    def _update(self):
        # ----- initialization ----------

        if not self._executed:
            self._num_of_executed_averaging_orders = {ticker: 0 for ticker in self.tickers_collection}
            self._lots_executed = {ticker: 0 for ticker in self.tickers_collection}
            self._current_sum_of_market_orders = {ticker: 0 for ticker in self.tickers_collection}
            self._threshold_price = {ticker: np.inf for ticker in self.tickers_collection}
            self._num_of_order = {ticker: 0 for ticker in self.tickers_collection}
            self._ticker_state = {ticker: 'calm' for ticker in self.tickers_collection}

        def create_averaging_orders(
                ticker: Ticker,
                current_sum_of_market_orders: float,
                number_of_filled_market_orders: int,
                number_of_order: int
        ):
            if ticker in self._selected_tickers_to_buy:
                direction = OrderDirection.ORDER_DIRECTION_BUY
                opposite_direction = OrderDirection.ORDER_DIRECTION_SELL
                current_sum_of_market_orders += self.portfolio_prices[ticker][direction]
                self._threshold_price[ticker] = (self.portfolio_prices[ticker][direction]
                                                 * (1 - self._return_threshold_down / 10000))

                desired_price = current_sum_of_market_orders / (number_of_filled_market_orders + 1) \
                                * (1 + self._return_threshold_up / 10000)
            elif ticker in self._selected_tickers_to_sell:
                direction = OrderDirection.ORDER_DIRECTION_SELL
                opposite_direction = OrderDirection.ORDER_DIRECTION_BUY
                current_sum_of_market_orders += self.portfolio_prices[ticker][direction]
                self._threshold_price[ticker] = (self.portfolio_prices[ticker][direction]
                                                 * (1 + self._return_threshold_up / 10000))

                desired_price = current_sum_of_market_orders / (number_of_filled_market_orders + 1) \
                                * (1 - self._return_threshold_down / 10000)

            return [
                LocalOrder(
                    order_name=f'market_{number_of_order}',
                    price=self.portfolio_prices[ticker][direction],
                    lots=self.portfolio_lots[ticker],
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
                    lots=self.portfolio_lots[ticker] * (number_of_filled_market_orders + 1),
                    direction=opposite_direction,
                    instrument_uid=ticker.uid,
                    ticker=ticker,
                    order_type=OrderType.ORDER_TYPE_LIMIT,
                    account_id=self._account.id,
                    order_id=''
                )
            ]

        def create_unwanted_order(ticker: Ticker, number_of_order: int, number_of_filled_market_orders: int):
            if ticker in self._selected_tickers_to_buy:
                direction = OrderDirection.ORDER_DIRECTION_BUY
                opposite_direction = OrderDirection.ORDER_DIRECTION_SELL
                undesired_price = (self.portfolio_prices[ticker][opposite_direction]
                                   * (1 - self._return_threshold_down / 10000))
            elif ticker in self._selected_tickers_to_sell:
                direction = OrderDirection.ORDER_DIRECTION_SELL
                opposite_direction = OrderDirection.ORDER_DIRECTION_BUY
                undesired_price = (self.portfolio_prices[ticker][opposite_direction]
                                   * (1 + self._return_threshold_up / 10000))
            return [
                LocalOrder(
                    order_name=f'unwanted_{number_of_order}',
                    price=undesired_price,
                    lots=self.portfolio_lots[ticker] * number_of_filled_market_orders,
                    direction=opposite_direction,
                    instrument_uid=ticker.uid,
                    ticker=ticker,
                    order_type=OrderType.ORDER_TYPE_MARKET,
                    account_id=self._account.id,
                    order_id=''
                )
            ]

        def forecast_next_state(ticker: Ticker):
            self._ticker_state[ticker] = self._ticker_pipelines[ticker].model.forecast_next_state()

        # -------- logic ---------------

        if not self._executed:
            self._ticker_pipelines = {
                ticker: Pipeline(
                    ticker=ticker
                ).make_pipeline(self._pipeline, end_date=self._period.time_period).load_model(
                    load_data=self._states_from_train_data)
                for ticker in self.tickers_collection
            }

            for pipe in self._ticker_pipelines.values():
                if self._states_from_train_data:
                    X = pipe.final_datanodes.data
                    returns = pipe.fetch_data('Returns').to_numpy()

                    pipe.model.determine_states(
                        X=X,
                        returns=returns,
                        returns_type=self._model_metadata['returns_type'],
                        t_threshold=self._t_threshold
                    )
                else:
                    pipe.model.determine_states(
                        returns_type=self._model_metadata['returns_type'],
                        t_threshold=self._t_threshold
                    )

            self._executed = True

        ## updating info on orders

        filled_orders = self._order_manager.update_relevant_orders()

        for order in filled_orders:
            if 'desired' in order.order_name or 'unwanted' in order.order_name:
                self._deselect_ticker(order.ticker)
            if 'market' in order.order_name:
                self._num_of_executed_averaging_orders[order.ticker] += 1
                self._current_sum_of_market_orders[order.ticker] += order.price

        ## updating info on candles

        for ticker in self._tickers_for_candle_fetching:
            new_candles_supplied = self._services.get_candles(ticker)

            if new_candles_supplied:
                self._ticker_pipelines[ticker].update(new_date=self._period.time_period)

                forecast_next_state(ticker)

        ## selecting new tickers for new trade

        staged_tickers_for_lots_computation = []

        for ticker in self._tickers_for_candle_fetching:
            if (ticker not in self._selected_tickers_to_trade
                    and len(self._selected_tickers_to_trade) < self._max_num_of_tickers):
                if self._ticker_state[ticker] in self._states_to_buy:
                    self._selected_tickers_to_trade.add(ticker)
                    staged_tickers_for_lots_computation.append(ticker)

                    self._selected_tickers_to_buy.add(ticker)
                elif self._ticker_state[ticker] in self._states_to_sell:
                    self._selected_tickers_to_trade.add(ticker)
                    staged_tickers_for_lots_computation.append(ticker)

                    self._selected_tickers_to_sell.add(ticker)

        ## updating prices for selected tickers and lots for newly selected tickers

        self._compute_portfolio_prices()

        for ticker in staged_tickers_for_lots_computation:
            self._compute_portfolio_lots_for_trade(ticker)

        ## creating averaging orders if the threshold is met

        for ticker in self._selected_tickers_to_trade:
            if ticker in self._selected_tickers_to_buy:
                sign = 1
                direction = OrderDirection.ORDER_DIRECTION_BUY
                opposite_direction = OrderDirection.ORDER_DIRECTION_SELL
            else:
                sign = -1
                direction = OrderDirection.ORDER_DIRECTION_SELL
                opposite_direction = OrderDirection.ORDER_DIRECTION_BUY

            if (self._threshold_price[ticker] is np.inf) and (sign == -1):
                self._threshold_price[ticker] = 0

            if sign * self.portfolio_prices[ticker][direction] <= sign * self._threshold_price[ticker]:
                self._order_manager.cancel_relevant_orders(
                    tickers=[ticker],
                    subname='desired'
                )

                if self._num_of_executed_averaging_orders[ticker] == self._num_of_averaging:
                    self._order_manager.add_new_orders(
                        create_unwanted_order(
                            ticker=ticker,
                            number_of_order=self._num_of_order[ticker],
                            number_of_filled_market_orders=self._num_of_executed_averaging_orders[ticker]
                        )
                    )
                else:
                    self._order_manager.add_new_orders(
                        create_averaging_orders(
                            ticker=ticker,
                            current_sum_of_market_orders=self._current_sum_of_market_orders[ticker],
                            number_of_filled_market_orders=self._num_of_executed_averaging_orders[ticker],
                            number_of_order=self._num_of_order[ticker]
                        )
                    )

                self._num_of_order[ticker] += 1

        ## post new orders

        self._trade()
