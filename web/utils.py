from tinkoff.invest import Quotation
from web.constants import candlePath
import os
import json


def process_nano(number):
    if type(number) is Quotation:
        number = {'units': number.units, 'nano': number.nano}

    to_number = number['units']

    for power, digit in enumerate(str(number['nano'])):
        to_number += int(digit) * 10**(-power)

    return to_number


def updateJson(ticker, **kwargs):
    tickerPath = candlePath + f'{ticker}/'

    if not os.path.isdir(tickerPath):
        os.mkdir(tickerPath)

    if os.path.isfile(tickerPath + f'{ticker}.json'):
        with open(tickerPath + f'{ticker}.json', 'r', encoding='utf-8') as f:
            jsonTicker = json.load(f)
            jsonTicker[ticker].update(kwargs)
    else:
        jsonTicker = {ticker: kwargs}

    with open(tickerPath + f'{ticker}.json', 'w', encoding='utf-8') as f:
        json.dump(jsonTicker, f, ensure_ascii=False, indent=4)

    return jsonTicker

#print(updateJson('KZIZP'))