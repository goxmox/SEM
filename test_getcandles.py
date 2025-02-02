from time import time
from api.tinvest.set_token import set_token
from api.tinvest.tclient import TClient

if __name__ == '__main__':
    #ticker = TTicker('SBER')
    set_token()

    start = time()

    with TClient(sandbox=False, trade=False) as client:
        client.services.get_instruments()
        client.services.save_candles()

    print(time() - start)
