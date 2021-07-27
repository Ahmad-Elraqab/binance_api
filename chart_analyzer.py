from os import close
from binance import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from config import API_KEY, API_SECRET, exchange_pairs


class SR:
    def __init__(self, isBottom, open, close, high, low):
        self.isBottom = isBottom
        self.open = open
        self.close = close
        self.high = high
        self.low = low


points_list = {}
support_list = {}
resistance_list = {}

client = Client(api_key=API_KEY, api_secret=API_SECRET)


def getData():
    for pair in exchange_pairs:
        klines = client.get_historical_klines(
            pair, client.KLINE_INTERVAL_4HOUR, "25 july 2021")

        num = 0
        points_list[pair] = []
        for idx, val in enumerate(klines):

            open = float(val[1])
            high = float(val[2])
            low = float(val[3])
            close = float(val[4])

            if num != 0:
                prev_open = float(klines[idx - 1][1])
                prev_high = float(klines[idx - 1][2])
                prev_low = float(klines[idx - 1][3])
                prev_close = float(klines[idx - 1][4])

                if (close / open >= 1 and prev_close / prev_open < 1) or (close / open < 1 and prev_close / prev_open >= 1):
                    obj = SR(isBottom=None, open=open,
                             close=close, high=high, low=low)
                    points_list[pair].append(obj)

            else:
                num = num + 1

        analyzePoint(pair, 36000.524)

# for key in points_list:
#     print(points_list[key])


def analyzePoint(symbol, current_price):

    resistance_list[symbol] = []
    support_list[symbol] = []

    for point in points_list[symbol]:

        if point.open > current_price:
            resistance_list[symbol].append(point.high)
        else:
            support_list[symbol].append(point.low)
            
    print(find_nearest(resistance_list[symbol], current_price))
    print(find_nearest(support_list[symbol], current_price))

def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return array[idx]


getData()
