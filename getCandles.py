from tinkoff.invest import Client, CandleInterval, RequestError
from web import get_token, process_nano, candlePath, instrumentPath, updateJson
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from time import sleep

token = get_token(trade=False)
candle_query_period = 365


def candles_writer(client, figi: str, from_: datetime, to=None):
    candle_dict = {'open': [], 'high': [], 'low': [], 'close': [], 'volume': [], 'time': []}
    success = False

    while not success:
        try:
            for candle in client.get_all_candles(figi=figi, from_=from_, to=to,
                                                 interval=CandleInterval.CANDLE_INTERVAL_1_MIN):
                candle_dict['open'].append(process_nano(candle.open))
                candle_dict['high'].append(process_nano(candle.high))
                candle_dict['low'].append(process_nano(candle.low))
                candle_dict['close'].append(process_nano(candle.close))
                candle_dict['volume'].append(candle.volume)
                candle_dict['time'].append(candle.time)
                from_ = candle.time

            success = True
        except RequestError as e:
            print(e.args[0], e.args[1], e.args[2])
            sleep(e.args[2].ratelimit_reset + 0.5)

    try:
        last_candle = candle_dict['time'][-1]
    except IndexError:
        return pd.DataFrame([]), from_

    print(last_candle + timedelta(minutes=1))
    return pd.DataFrame(candle_dict), last_candle + timedelta(minutes=1)


with Client(token) as client:
    shares = pd.read_csv(instrumentPath + 'shares.csv')[['figi', 'ticker', 'first_1min_candle_date']]
    futures = pd.read_csv(instrumentPath + 'futures.csv')[['figi', 'ticker', 'first_1min_candle_date']]

    instruments = pd.concat([shares, futures])

    for idx, instrument in instruments.iterrows():
        all_candles = []
        ticker, firstCandle = instrument['ticker'], instrument['first_1min_candle_date']
        last_colon_idx = firstCandle.rfind(':')
        tickerPath = candlePath + f"{ticker}/"

        if os.path.isdir(tickerPath):
            json_candle_date = updateJson(ticker)
        else:
            json_candle_date = updateJson(ticker, last_candle=firstCandle[:last_colon_idx] + firstCandle[last_colon_idx + 1:])
        print(json_candle_date)

        start_date = datetime.strptime(
            json_candle_date[ticker]['last_candle'], '%Y-%m-%d %H:%M:%S%z')

        cur_date = datetime.now().astimezone()
        while (cur_date - start_date).days > candle_query_period:
            candles, start_date = candles_writer(client, instrument['figi'], from_=start_date,
                                                 to=start_date + timedelta(days=candle_query_period))

            if candles.shape[0] > 0:
                all_candles.append(candles)
            cur_date = datetime.now().astimezone()

        candles, last_candle = candles_writer(client, instrument['figi'], from_=start_date)
        if candles.shape[0] > 0:
            all_candles.append(candles)

        try:
            all_candles = pd.concat(all_candles)
        except ValueError:
            continue

        all_candles.to_csv(tickerPath + f'{ticker}.csv', mode='a', index=False,
                           header=not os.path.isfile(tickerPath + f'{ticker}.csv'))

        updateJson(ticker, last_candle=last_candle.strftime('%Y-%m-%d %H:%M:%S%z'))
