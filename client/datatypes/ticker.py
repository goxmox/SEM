import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, date
from web import candlePath, updateJson, holidays, workingWeekends, getWorkingHours
from client.quote_workers import realizedMeasures, candlesToReturns
import os
import pickle
from typing import Callable
from numba import njit

@njit
def createPriceMatrix(priceArr, candleDays, getWorkingHours):
    numOfCandles = np.zeros(shape=len(candleDays))

    for i in range(len(candleDays)):
        numOfCandles[i] = (getWorkingHours(candleDays[i])['close'] - getWorkingHours(candleDays[i])['open'])


def getPrices(ticker: str, candleToPrice: str | Callable = 'close'):
    if candleToPrice in ['close', 'open', 'high', 'low']:
        candleToPrice = lambda x: x[candleToPrice]

    # open candles
    if os.path.isfile(candlePath + f'{ticker}/{ticker}.csv'):
        candles = pd.read_csv(candlePath + f'{ticker}/{ticker}.csv')
    else:
        raise OSError(f'No {ticker}.csv file')

    candles['prices'] = candles.apply(candlesToReturns, axis=1)
    candles.loc[:, 'time'] = pd.to_datetime(candles['time'], format='%Y-%m-%d %H:%M:%S%z')
    candleDays = candles['time'].apply(lambda x: x.date()).unique()

    priceArr = candles[['prices', 'time']].to_numpy()



class Prices:
    ticker: str
    lastCandleDate: datetime
    startOfDayIdx: list[int]
    prevDayPrice: list[float]
    days: list[date]

    def __init__(self, ticker: str, candleToPrice: str | Callable = 'close'):
        self.ticker = ticker
        candles: pd.DataFrame

        # process candleToPrice
        if candleToPrice in ['close', 'open', 'high', 'low']:
            candleToPrice = lambda x: x[candleToPrice]

        # open candles
        if os.path.isfile(candlePath + f'{self.ticker}/{self.ticker}.csv'):
            candles = pd.read_csv(candlePath + f'{self.ticker}/{self.ticker}.csv')
        else:
            raise OSError(f'No {self.ticker}.csv file')

        # open json of a ticker
        if os.path.isfile(candlePath + f'{self.ticker}/{self.ticker}.json'):
            jsonCandles = updateJson(self.ticker)
        else:
            raise OSError(f'No {self.ticker}.json file')

        self.lastCandleDate = datetime.strptime(jsonCandles[self.ticker]['last_candle'], '%Y-%m-%d %H:%M:%S%z')

        # loading previously computed attributes
        if 'saved_candles_idx' in jsonCandles[self.ticker].keys():
            idx = jsonCandles[self.ticker]['saved_candles_idx']

            with open(candlePath + f'{self.ticker}/{self.ticker}_meta_candles.pkl', 'rb') as candlesPickle:
                savedCandles = pickle.load(candlesPickle)

            self.startOfDayIdx = savedCandles['startOfDayIdx']
            self.prevDayPrice = savedCandles['prevDayPrice']
            self.days = savedCandles['days']
        else:
            self.startOfDayIdx, self.prevDayPrice, self.days = [], [], []
            idx = 0

        # updating the json with the new idx value and removing unnecessary data
        updateJson(self.ticker, saved_candles_idx=candles.shape[0])

        candles['prices'] = candles.apply(candlesToReturns, axis=1)
        candles.loc[:, 'time'] = pd.to_datetime(candles['time'], format='%Y-%m-%d %H:%M:%S%z')
        numOfDays = len(candles['time'].apply(lambda x: x.date()).unique())

        priceArr = candles[['prices', 'time']].to_numpy()

        if idx >= candles.shape[0]:
            # pd.DataFrame is used in order to seamlessly skip the loop over candles.iterrows() below
            candles = pd.DataFrame()
        else:
            candles = candles.iloc[idx:, :].reset_index(drop=True)

        # initialize these variables only when there are new candles to process
        if candles.shape[0] > 0:
            day, prevPrice = candles.loc[0, 'time'], candles.loc[0, 'close']
            day = datetime.strptime(day[:day.find(' ')], '%Y-%m-%d').date()

        # here we run over all available candles to populate the startOfDayIdx list with
        # indices that point to the start of each trading day
        for idx, candle in candles.iterrows():
            candleDay = candle.loc['time']
            candleDay = datetime.strptime(candleDay[:candleDay.find(' ')], '%Y-%m-%d').date()

            # enter if-statement when the new day is found
            if candleDay != day:
                day = candleDay

                self.days.append(day)
                self.startOfDayIdx.append(idx)
                self.prevDayPrice.append(prevPrice)

            prevPrice = candle.loc['close']

        # saving computed attributes
        with open(candlePath + f'{self.ticker}/{self.ticker}_meta_candles.pkl', 'wb') as candlesPickle:
            pickle.dump({
                'startOfDayIdx': self.startOfDayIdx,
                'prevDayPrice': self.prevDayPrice,
                'days': self.days
            }, candlesPickle)

    def getPrices(self, offsetDaysFromEnd: int = None,
                  offsetDaysFromStart: int = None) -> np.ndarray:
        candles = pd.read_csv(candlePath + f'{self.ticker}/{self.ticker}.csv')

        price = []
        candleDay = datetime.strptime(candles.iloc[0]['time'][:candles.iloc[0]['time'].find(' ')], '%Y-%m-%d').date()
        workingHours = getWorkingHours(candleDay)

        curTime = workingHours['open']
        for idx, candle in candles.iterrows():
            candleTime = datetime.strptime(candle['time'][candle['time'].find(' ') + 1:candle['time'].find('+')],
                                           '%H:%M:%S').time()

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
        return np.log(price[1:] / price[:-1])


@dataclass
class Ticker:
    ticker: str

    def __post_init__(self):
        self.candles = Prices(self.ticker)

    def rmQuery(self, rms: list[str] = ['rv'], ignore_weekends=True) -> pd.DataFrame:
        tickerJson = updateJson(self.ticker)
        candles = pd.read_csv(candlePath + f'{self.ticker}/{self.ticker}.csv')

        # check whether the realized measure has an implementation under the listed name
        rms = [rm for rm in rms if rm in realizedMeasures.keys()]
        if len(rms) == 0:
            raise ValueError('No implemented realized measure requested')

        # get last saved candle dates for each realized measure
        lastRmsDays = {rm: date.min for rm in rms}
        rmValues = {rm: [] for rm in rms}
        if 'rms' in tickerJson[self.ticker].keys():
            lastRmsDaysMem = tickerJson[self.ticker]['rms']

            for rm in lastRmsDaysMem.keys():
                if rm in rms:
                    lastRmsDays[rm] = datetime.strptime(lastRmsDaysMem[rm], '%Y-%m-%d').date()
                    with open(candlePath + f'{self.ticker}/{self.ticker}_{rm}.pkl', 'rb') as rmPickle:
                        rmValues[rm] = pickle.load(rmPickle)

        # compute indices of days from which we start computing new realized measures
        idxDays = {rm: 0 for rm in rms}
        for rm in rms:
            while idxDays[rm] < len(self.candles.days) and lastRmsDays[rm] >= self.candles.days[idxDays[rm]]:
                idxDays[rm] += 1

        # compute new values of realized measures
        for dayIdx in range(min(idxDays.values()), len(self.candles.days) - 1):
            print(dayIdx)

            # filter weekends and holidays (some weekends are working days, we do not filter those days)
            if (((ignore_weekends and self.candles.days[dayIdx] not in workingWeekends) and
                    self.candles.days[dayIdx].weekday() >= 5) or self.candles.days[dayIdx] in holidays):
                print('weekend')
                continue

            dayReturns = candlesToReturns(candles.iloc[self.candles.startOfDayIdx[dayIdx]:
                                                       self.candles.startOfDayIdx[dayIdx + 1]],
                                          self.candles.prevDayPrice[dayIdx])

            for rm in rms:
                # if for the day pointed by dayIdx the realized measure was never computed, then add it to rmValues
                if idxDays[rm] <= dayIdx:
                    rmValues[rm].append(realizedMeasures[rm](dayReturns))
                    # updating the last candle day (doing it here, and not further down the code to avoid weekends,
                    # as this loop avoids weekends)
                    lastRmsDays[rm] = self.candles.days[dayIdx]

        # dump new values and cast lastRmsDays to the string value
        for rm in rms:
            with open(candlePath + f'{self.ticker}/{self.ticker}_{rm}.pkl', 'wb') as rmPickle:
                pickle.dump(rmValues[rm], rmPickle)

            lastRmsDays[rm] = lastRmsDays[rm].strftime('%Y-%m-%d')

        # update json with new candle dates
        if 'rms' in tickerJson[self.ticker].keys():
            lastRmsDaysMem.update(lastRmsDays)
            updateJson(self.ticker, rms=lastRmsDaysMem)
        else:
            updateJson(self.ticker, rms=lastRmsDays)

        # return all computed rms
        return pd.DataFrame(rmValues)
