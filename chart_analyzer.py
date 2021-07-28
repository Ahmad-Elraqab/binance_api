from binance import Client
import numpy as np
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import Client
import pandas as pd
import numpy as np


points_list = {}
support_list = {}
resistance_list = {}

current_rs = {
    'support': 0.0,
    'resistance': 0.0
}

client = Client(api_key=API_KEY, api_secret=API_SECRET)

tickers = client.get_all_tickers()


def isSupport(df, i):
    try:
        support = float(df[i][3]) < float(df[i-1][3]) and float(df[i][3]) < float(df[i+1][3]) \
            and float(df[i+1][3]) < float(df[i+2][3]) and float(df[i-1][3]) < float(df[i-2][3])

        return support

    except:

        print("Invalid Index")


def isResistance(df, i):

    try:
        resistance = float(df[i][2]) > float(df[i-1][2]) and float(df[i][2]) > float(df[i+1][2]) \
            and float(df[i+1][2]) > float(df[i+2][2]) and float(df[i-1][2]) > float(df[i-2][2])

        return resistance

    except:

        print("Invalid Index")


def getData(interval):

    for pair in exchange_pairs:
        klines = client.get_historical_klines(
            pair, Client.KLINE_INTERVAL_4HOUR, "1 oct 2020")

        points_list[pair] = {}
        points_list[pair][interval] = []

        for i, value in enumerate(klines):

            if isSupport(klines, i):
                l = klines[i][3]
                points_list[pair][interval].append(l)

            elif isResistance(klines, i):
                h = klines[i][2]
                points_list[pair][interval].append(h)


def getInterval(interval):
    if interval == "1D":
        return Client.KLINE_INTERVAL_1DAY
    elif interval == "12hr":
        return Client.KLINE_INTERVAL_12HOUR
    elif interval == "8hr":
        return Client.KLINE_INTERVAL_8HOUR
    elif interval == "6hr":
        return Client.KLINE_INTERVAL_6HOUR
    elif interval == "4hr":
        return Client.KLINE_INTERVAL_4HOUR
    elif interval == "1hr":
        return Client.KLINE_INTERVAL_1HOUR
    elif interval == "30m":
        return Client.KLINE_INTERVAL_30MINUTE
    elif interval == "15m":
        return Client.KLINE_INTERVAL_15MINUTE
    elif interval == "5m":
        return Client.KLINE_INTERVAL_5MINUTE
    elif interval == "3m":
        return Client.KLINE_INTERVAL_30MINUTE
    elif interval == "1m":
        return Client.KLINE_INTERVAL_1MINUTE


def analyzePoint(interval, symbol, current_price):

    support_list[symbol] = {}
    resistance_list[symbol] = {}

    support_list[symbol][interval] = []
    resistance_list[symbol][interval] = []

    for point in points_list[symbol][interval]:

        if point > current_price:

            resistance_list[symbol][interval].append(float(point))

        else:

            support_list[symbol][interval].append(float(point))

    current_rs['support'] = find_nearest(
        support_list[symbol][interval], current_price)
    current_rs['resistance'] = find_nearest(
        resistance_list[symbol][interval], current_price)


def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - float(value))).argmin()
    return array[idx]
