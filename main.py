import pandas as pd
import numpy as np
#from client import Ticker
#from client.models import Models, HAR_RV
from timeit import default_timer as timer

shares = pd.read_csv('/Users/s/Desktop/segr/instruments/shares.csv')[['figi', 'ticker', 'first_1min_candle_date']]
futures = pd.read_csv('/Users/s/Desktop/segr/instruments/futures.csv')[['figi', 'ticker', 'first_1min_candle_date']]

instruments = pd.concat([shares, futures])

print(list(instruments['ticker']))

start = timer()

#sber = Models(['SBER', 'MGNT', 'VKCO'], [HAR_RV(), HAR_RV(transformRV=np.sqrt)]).getTestPredict()

check1 = timer()

#print(sber.shape, check1 - start)

    

