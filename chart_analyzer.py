from binance import Client
import numpy as np
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import Client
import pandas as pd
import numpy as np
import csv

points_list = {}
support_list = {}
resistance_list = {}

current_rs = {}

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
            pair, getInterval(interval), "1 july 2021")

        points_list[pair] = {}
        points_list[pair][interval] = []

        for i, value in enumerate(klines):

            if isSupport(klines, i):
                l = klines[i][3]
                points_list[pair][interval].append(
                    {'pair': pair, 'date': pd.to_datetime(klines[i][0], unit='ms'), 'price': l})
                # print("support at \t\t", l, "\t\t",
                #       pd.to_datetime(klines[i][0], unit='ms'))

            elif isResistance(klines, i):
                h = klines[i][2]
                points_list[pair][interval].append(
                    {'pair': pair, 'date': pd.to_datetime(klines[i][0], unit='ms'), 'price': h})
                # print("resistance at \t\t", h, "\t\t",
                #       pd.to_datetime(klines[i][0], unit='ms'))

        data = pd.DataFrame(points_list[pair][interval])
        data.to_csv(f'files/{interval}.csv', mode='a',
                    index=False, header=False)


def loadDate(interval):
    with open(f'files/{interval}.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0

        for row in csv_reader:

            if not row[0] in points_list:
                points_list[row[0]] = {}
                points_list[row[0]][interval] = []
                points_list[row[0]][interval].append(row[2])
            else:
                points_list[row[0]][interval].append(row[2])

            line_count += 1

    # print(points_list['KMDUSDT'][interval])


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

    # print(points_list[symbol][interval])
    for point in points_list[symbol][interval]:

        if point > current_price:

            resistance_list[symbol][interval].append(float(point))

        else:

            support_list[symbol][interval].append(float(point))

    current_rs[symbol] = {}
    current_rs[symbol]['support'] = find_nearest(
        support_list[symbol][interval], current_price)
    current_rs[symbol]['resistance'] = find_nearest(
        resistance_list[symbol][interval], current_price)

    print(symbol, "\t support\t", current_rs[symbol]['support'])
    print(symbol, "\t resistance\t", current_rs[symbol]['resistance'])


def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - float(value))).argmin()
    return array[idx]


def tech_1(interval, symbol, current_price):

    resistance_list[symbol][interval] 
# getData("30m")



class Order:
    def __init__(self, price, stopLose, sellPercent, resistance):
        self.price = price
        self.stopLose = stopLose
        self.sellPercent = sellPercent

    
    def sell():
        print("sell now")

    def buy():
        print("buy now")



class Node:
   def __init__(self, dataval=None):
      self.dataval = dataval
      self.nextval = None
      self.backval = None

# class SLinkedList:
#    def __init__(self):
#       self.headval = None

# list1 = SLinkedList()
# list1.headval = Node("Mon")
# e2 = Node("Tue")
# e3 = Node("Wed")
# # Link first Node to second node
# list1.headval.nextval = e2

# # Link second Node to third node
# e2.nextval = e3