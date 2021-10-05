from os import close
from time import time
from models.order import Order
from matplotlib import pyplot as plt
from binance import Client
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import AsyncClient, Client
from datetime import datetime
from binance.streams import ThreadedWebsocketManager
import numpy as np
import pandas as pd
import numpy as np
import csv
import xlsxwriter
import mplfinance as mpf


points_list = {}
support_list = {}
resistance_list = {}
current_rs = {}
isSet = False

client = Client(api_key=API_KEY, api_secret=API_SECRET)
tickers = client.get_all_tickers()


class Order:
    def __init__(self):
        self.id = 'id'
        self.isSet = False
        self.isBuy = False
        self.goal1 = None
        self.goal2 = None
        self.stoplose = None
        self.head = Node(value=0)


class Node:
    def __init__(self, value):

        self.next = None
        self.before = None
        self.value = value

    def pushAfter(self, node):

        head = self
        while head.next is not None:

            head = head.next

        head.next = node

    def pushBefore(self, node):

        head = self

        temp = head.before

        head.before = node

        node.before = temp

    def printNext(self):

        head = self

        print('--Resistance--')
        while head.next is not None:

            print(head.next.value)

            head = head.next

        print('--------------')

    def printBefore(self):

        head = self

        print('----Support----')
        while head.before is not None:

            print(head.before.value)

            head = head.before

        print('---------------')


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

    for index, pair in enumerate(exchange_pairs):
        klines = client.get_historical_klines(
            pair, getInterval(interval), "1 aug 2021")

        points_list[pair] = {}
        points_list[pair][interval] = []

        df = pd.DataFrame(klines)
        df[0] = pd.to_datetime(df[0], unit='ms')

        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                      'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

        df = df.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

        df = df.set_index('Date')
        df['Close'] = pd.to_numeric(
            df['Close'], errors='coerce')
        df['Open'] = pd.to_numeric(
            df['Open'], errors='coerce')
        df['High'] = pd.to_numeric(
            df['High'], errors='coerce')
        df['Low'] = pd.to_numeric(
            df['Low'], errors='coerce')
        df['Volume'] = pd.to_numeric(
            df['Volume'], errors='coerce')

        sr = pd.DataFrame(columns=['pair', 'Date', 'price'])

        # list = []

        for i, value in enumerate(klines):

            if isSupport(klines, i):
                l = klines[i][3]
                sr = sr.append(
                    {'pair': pair,
                     'Date': pd.to_datetime(klines[i][0], unit='ms'),
                     'price': l
                     }, ignore_index=True
                )
                # list.append(pd.to_numeric(l))

            elif isResistance(klines, i):
                h = klines[i][2]
                sr = sr.append(
                    {'pair': pair,
                     'Date': pd.to_datetime(klines[i][0], unit='ms'),
                     'price': h
                     }, ignore_index=True
                )
                # list.append(pd.to_numeric(h))

        sr = sr.sort_values(["price"], ascending=True)
        sr.to_csv(f'SR/{pair}@ticker.csv', mode='a',
                  index=False, header=False)

        # pd.to_numeric(df['Close']).plot()
        # pd.to_numeric(sr['price']).plot()
        # mpf.plot(df, type='candle', style='yahoo',
        #          volume=True, hlines=list)
        # plt.show()

    print("DONE")


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

    return points_list


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


def load_sr(symbol, price):

    if points_list[symbol]['order'].isSet == False:
        print('True')
        with open(f'SR/{symbol}@ticker.csv') as csv_file:

            csv_reader = csv.reader(csv_file, delimiter=',')

            for row in csv_reader:

                node = Node(value=row[2])
                if row[2] > price:
                    points_list[symbol]['order'].head.pushAfter(node)
                else:
                    points_list[symbol]['order'].head.pushBefore(node)

            points_list[symbol]['order'].isSet = True


def init():
    for pair in exchange_pairs:
        points_list[pair] = {}
        points_list[pair]['time'] = {}
        points_list[pair]['order'] = Order()



def handle_price_socket(msg):

    # 1 - load prev orders.
    # 2 - load pairs SR and set them.
    load_sr(msg['k']['s'], msg['k']['c'])

    # 3 - detect new orders.

    time = msg['k']['t']
    symbol = msg['k']['s']
    close = msg['k']['c']

    if time in points_list[symbol]['time']:

        print('exist!')
        print(points_list)

    else:

        points_list[symbol]['time'][time] = close


init()

twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)

twm.start()

for pair in exchange_pairs:
    twm.start_kline_socket(
        callback=handle_price_socket, symbol=pair, interval=AsyncClient.KLINE_INTERVAL_5MINUTE)
twm.join()

# getData('1D')
