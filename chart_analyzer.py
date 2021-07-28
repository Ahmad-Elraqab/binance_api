from os import close
from binance import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from config import API_KEY, API_SECRET, exchange_pairs

from binance.client import Client
import pandas as pd
import numpy as np


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
levels = []
client = Client(api_key=API_KEY, api_secret=API_SECRET)

tickers = client.get_all_tickers()


def isSupport(df, i):
    support = float(df[i][3]) < float(df[i-1][3]) and float(df[i][3]) < float(df[i+1][3]) \
        and float(df[i+1][3]) < float(df[i+2][3]) and float(df[i-1][3]) < float(df[i-2][3])

    return support


def isResistance(df, i):
    resistance = float(df[i][2]) > float(df[i-1][2]) and float(df[i][2]) > float(df[i+1][2]) \
        and float(df[i+1][2]) > float(df[i+2][2]) and float(df[i-1][2]) > float(df[i-2][2])

    return resistance


def getData(pair):

    klines = client.get_historical_klines(
        pair, client.KLINE_INTERVAL_1DAY, "1 oct 2020")

    for i, value in enumerate(klines):

        if not ((i == 0) or (i == 1) or i == len(klines) or i == len(klines) - 1):
            if isSupport(klines, i):
                l = klines[i][3]
                print("this is a support =>\t\t",l)
    
            elif isResistance(klines, i):
                h = klines[i][2]
                print("this is a resistance =>\t\t",h)

    
getData('BTCUSDT')
