import numpy as np
from web import getWorkingHours
from datetime import datetime, timedelta, time


def candlesToReturns(candles, lastPrice):
    price = []
    candleDay = datetime.strptime(candles.iloc[0]['time'][:candles.iloc[0]['time'].find(' ')], '%Y-%m-%d').date()
    workingHours = getWorkingHours(candleDay)

    curTime = workingHours['open']
    for idx, candle in candles.iterrows():
        candleTime = datetime.strptime(candle['time'][candle['time'].find(' ')+1:candle['time'].find('+')], '%H:%M:%S').time()

        if candleTime < workingHours['open']:
            lastPrice = candle['close']
            continue

        while candleTime >= curTime > time():
            price.append(lastPrice)
            if candleTime == curTime:
                lastPrice = candle['close']

            curTime = (datetime.combine(candleDay, curTime) + timedelta(minutes=1)).time()

    price = np.array(price)
   # raise ValueError
    return np.log(price[1:]/price[:-1])

