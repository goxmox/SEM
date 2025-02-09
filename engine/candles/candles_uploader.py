import pandas as pd
from engine.schemas.constants import candle_path, instrument_path
from engine.schemas.datatypes import Ticker, Broker
import os
import json
from datetime import timedelta, datetime


class LocalCandlesUploader:
    candles_in_memory: dict[Ticker, pd.DataFrame] = {}

    broker: Broker

    new_candles: dict[Ticker, list[pd.DataFrame]] = {}
    candles_start_dates: dict[Ticker, datetime] = {}
    last_candles: dict[Ticker, pd.DataFrame] = {}

    @staticmethod
    def upload_candles(ticker: Ticker):
        if ticker in LocalCandlesUploader.candles_in_memory.keys():
            return LocalCandlesUploader.candles_in_memory[ticker]
        else:
            candles_df = pd.read_csv(
                candle_path + f'{LocalCandlesUploader.broker.broker_name}/{ticker.ticker_sign}/{ticker.ticker_sign}.csv'
            )

            candles_df['time'] = pd.to_datetime(candles_df['time'], format='%Y-%m-%d %H:%M:%S%z')
            candles_df = candles_df.set_index('time')

            LocalCandlesUploader.candles_in_memory[ticker] = candles_df

            last_candle = candles_df.iloc[-1:].copy()

            LocalCandlesUploader.last_candles[ticker] = last_candle
            LocalCandlesUploader.candles_start_dates[ticker] = last_candle.index[0] \
                                                                           + timedelta(minutes=1)

            return candles_df

    @staticmethod
    def get_last_candle(ticker: Ticker):
        if ticker in LocalCandlesUploader.last_candles.keys():
            return LocalCandlesUploader.last_candles[ticker]
        else:
            return None

    @staticmethod
    def get_new_candle_datetime(ticker: Ticker):
        if ticker in LocalCandlesUploader.candles_start_dates.keys():
            return LocalCandlesUploader.candles_start_dates[ticker]
        else:
            instruments_first_candles = pd.read_csv(
                instrument_path + f'{LocalCandlesUploader.broker.broker_name}/{ticker.type_instrument.name}.csv'
            )[['uid', 'first_1min_candle_date']].set_index('uid')

            start_date = datetime.strptime(
                instruments_first_candles.loc[ticker.uid, 'first_1min_candle_date'],
                '%Y-%m-%d %H:%M:%S%z'
            )

        return start_date

    @staticmethod
    def save_new_candles(new_candles: pd.DataFrame, ticker: Ticker):
        if ticker in LocalCandlesUploader.new_candles.keys():
            LocalCandlesUploader.new_candles[ticker].append(new_candles)
        else:
            LocalCandlesUploader.new_candles[ticker] = [new_candles]

    @staticmethod
    def cache_new_candles():
        for ticker in LocalCandlesUploader.new_candles.keys():
            ticker_path = candle_path + f"{LocalCandlesUploader.broker.broker_name}/{ticker.ticker_sign}/"

            new_candles = LocalCandlesUploader.new_candles[ticker]

            if len(new_candles) > 0:
                if not os.path.isdir(ticker_path):
                    os.makedirs(ticker_path)

                pd.concat(new_candles).to_csv(
                    ticker_path + f'{ticker.ticker_sign}.csv',
                    mode='a',
                    header=not os.path.isfile(
                        ticker_path + f'{ticker.ticker_sign}.csv'
                    )
                )

            if ticker in LocalCandlesUploader.candles_in_memory.keys():
                LocalCandlesUploader.candles_in_memory[ticker] = pd.concat(
                    [LocalCandlesUploader.candles_in_memory[ticker]]
                    + LocalCandlesUploader.new_candles[ticker]
                )
            else:
                LocalCandlesUploader.candles_in_memory[ticker] = pd.concat(
                    LocalCandlesUploader.new_candles[ticker]
                )

        LocalCandlesUploader.new_candles = {}

