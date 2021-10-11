from os import close
import datetime
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

points_list = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)

mult1 = 0.500
mult2 = 1.000
mult3 = 1.500
mult4 = 2.000
mult5 = 2.500
mult6 = 3.000
mult7 = 3.500
mult8 = 4.000


def history():
 
        klines = client.get_historical_klines(
            symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_30MINUTE, start_str="27 jan 2021")

        data = pd.DataFrame(klines)

        data[0] = pd.to_datetime(data[0], unit='ms')

        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                        'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

        data = data.drop(columns=['IGNORE',
                                'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

        data['Close'] = pd.to_numeric(
            data['Close'], errors='coerce')

        data['Close'] = pd.to_numeric(data['Close'])
        data['High'] = pd.to_numeric(data['High'])
        data['Low'] = pd.to_numeric(data['Low'])
        data['Volume'] = pd.to_numeric(data['Volume'])
        data['sumpv'] = np.double
        data['sumv'] = np.double
        data['vwap'] = np.double
        start = pd.Timestamp(year=2021, month=1, day=27,
                            hour=0, minute=0, second=0)

        for key, candle in data.iterrows():
            data.iloc[key, data.columns.get_loc('sumpv')] = ((candle['Close'] + candle['High'] +
                                                            candle['Low']) / 3) * candle['Volume']
            data.iloc[key, data.columns.get_loc('sumv')] = candle['Volume']

            if candle['Date'] == start:

                data.iloc[key, data.columns.get_loc('vwap')] = np.round(
                    data.iloc[key, data.columns.get_loc('sumpv')] / data.iloc[key, data.columns.get_loc('sumv')], 2)

            else:

                n = key - 1
                while n >= 0:

                    data.iloc[key, data.columns.get_loc(
                        'sumpv')] += data.iloc[n, data.columns.get_loc('sumpv')]
                    data.iloc[key, data.columns.get_loc(
                        'sumv')] += data.iloc[n, data.columns.get_loc('sumv')]

                    n -= 1

                data.iloc[key, data.columns.get_loc('vwap')] = np.round(
                    data.iloc[key, data.columns.get_loc('sumpv')] / data.iloc[key, data.columns.get_loc('sumv')], 2)

            print(data.iloc[key, data.columns.get_loc('vwap')])
        data.to_csv(f'data@vwap.csv', mode='a',
                    index=False)


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

        # if close > after set limt buy order
        # elif close < before reset SR

        # check list of limt buy order
        # check list orders to sell and close


# init()

# twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)

# twm.start()

# for pair in exchange_pairs:
#     twm.start_kline_socket(
#         callback=handle_price_socket, symbol=pair, interval=AsyncClient.KLINE_INTERVAL_5MINUTE)
# twm.join()

# getData('1D')


history()

# sumpv := start_time_range ? hlc3 * volume : sumpv[1] + (hlc3*volume)
# sumv := start_time_range ? volume : sumv[1] + volume

# avwap = sumpv / sumv

# plot(avwap, color=avwapcol)

# int index = na
# index := start_time_range ? 1 : index[1] + 1

# float psum = na
# psum := start_time_range ? hlc3 : psum[1] + hlc3

# mean = psum / index

# float v1 = na
# v1 := start_time_range ? pow(hlc3-mean,2) : v1[1] + pow(hlc3-mean,2)
# variance = v1 / (index -1)
# dev = sqrt(variance)
