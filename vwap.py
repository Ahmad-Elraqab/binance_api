from os import close
import datetime
import threading
import time

from pandas.core.frame import DataFrame
from client import send_message
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

df = {}
orderList = {}
preOrder = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)

mult1 = 0.500
mult2 = 1.000
mult3 = 1.500
mult4 = 2.000
mult5 = 2.500
mult6 = 3.000
mult7 = 3.500
mult8 = 4.000


class Order:
    def __init__(self):
        self.id = 'id'
        self.isSet = False
        self.isTouch = False
        self.isBuy = False
        self.isOrder = False
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

    def deleteAllNextNodes(self):

        head = self

        while (head.next != None):

            temp = head.next
            head = head.next
            temp = None

    def deleteAllBeforeNodes(self):

        head = self

        while (head.before != None):

            temp = head.before
            head = head.before
            temp = None


class Book:
    def __init__(self, type, symbol, interval, buyPrice, amount, startDate):

        self.type = type
        self.isSold = False
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.amount = amount
        self.startDate = startDate
        self.sellPrice = None
        self.rate = None
        self.total = buyPrice
        self.endDate = None


def history(symbol):

    klines = client.get_historical_klines(
        symbol=symbol, interval=Client.KLINE_INTERVAL_2HOUR, start_str="27 jan 2021")

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
    # data = data.drop(data.index[-1])
    data['sumpv'] = ((data['Close'] + data['High'] +
                     data['Low']) / 3) * data['Volume']
    data['sumv'] = data['Volume']
    data['vwap'] = float()
    data['index'] = 1
    data['mean'] = float()
    data['psum'] = float()
    data['dev'] = float()
    data['v1'] = float()
    data['varince'] = float()
    data['sd1_pos'] = float()
    data['sd1_neg'] = float()
    data['sd2_pos'] = float()
    data['sd2_neg'] = float()
    data['sd3_pos'] = float()
    data['sd3_neg'] = float()
    data['sd4_pos'] = float()
    data['sd4_neg'] = float()
    data['sd5_pos'] = float()
    data['sd5_neg'] = float()
    data['sd6_pos'] = float()
    data['sd6_neg'] = float()
    data['sd7_pos'] = float()
    data['sd7_neg'] = float()
    data['sd8_pos'] = float()
    data['sd8_neg'] = float()
    data['hlc3'] = (data['Close'] + data['High'] + data['Low']) / 3

    df[symbol] = data

    for i in df[symbol].index:

        df[symbol].iloc[i, df[symbol].columns.get_loc('index')] = i + 1

        vwap(i, symbol)


def vwap(i, symbol):
    portion = df[symbol][:i+1]

    df[symbol].iloc[i, df[symbol].columns.get_loc(
        'sumv')] = df[symbol].iloc[i, df[symbol].columns.get_loc('Volume')]
    df[symbol].iloc[i, df[symbol].columns.get_loc('sumpv')] = ((df[symbol].iloc[i, df[symbol].columns.get_loc('Close')] + df[symbol].iloc[i, df[symbol].columns.get_loc(
        'High')]+df[symbol].iloc[i, df[symbol].columns.get_loc('Low')]) / 3) * df[symbol].iloc[i, df[symbol].columns.get_loc('Volume')]
    df[symbol].iloc[i, df[symbol].columns.get_loc('hlc3')] = (df[symbol].iloc[i, df[symbol].columns.get_loc(
        'Close')] + df[symbol].iloc[i, df[symbol].columns.get_loc('High')]+df[symbol].iloc[i, df[symbol].columns.get_loc('Low')]) / 3

    df[symbol].iloc[i, df[symbol].columns.get_loc('vwap')] = np.round(
        portion['sumpv'].sum() / portion['sumv'].sum(), 2)
    df[symbol].iloc[i, df[symbol].columns.get_loc('psum')] = df[symbol].iloc[i - 1, df[symbol].columns.get_loc(
        'psum')] + df[symbol].iloc[i, df[symbol].columns.get_loc('hlc3')]
    df[symbol].iloc[i, df[symbol].columns.get_loc('mean')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'psum')] / df[symbol].iloc[i, df[symbol].columns.get_loc('index')]
    df[symbol].iloc[i, df[symbol].columns.get_loc('v1')] = np.power(df[symbol].iloc[i, df[symbol].columns.get_loc(
        'hlc3')] - df[symbol].iloc[i, df[symbol].columns.get_loc('mean')], 2) + df[symbol].iloc[i - 1, df[symbol].columns.get_loc('v1')]
    df[symbol].iloc[i, df[symbol].columns.get_loc('varince')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'v1')] / (df[symbol].iloc[i, df[symbol].columns.get_loc('index')] - 1)
    df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] = np.sqrt(
        df[symbol].iloc[i, df[symbol].columns.get_loc('varince')])

    df[symbol].iloc[i, df[symbol].columns.get_loc('sd1_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult1)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd1_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult1)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd2_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult2)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd2_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult2)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd3_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult3)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd3_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult3)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd4_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult4)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd4_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult4)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd5_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult5)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd5_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult5)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd6_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult6)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd6_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult6)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd7_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult7)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd7_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult7)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd8_pos')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] + (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult8)
    df[symbol].iloc[i, df[symbol].columns.get_loc('sd8_neg')] = df[symbol].iloc[i, df[symbol].columns.get_loc(
        'vwap')] - (df[symbol].iloc[i, df[symbol].columns.get_loc('dev')] * mult8)


def updateLastRow(msg):

    close = pd.to_numeric(msg['k']['c'])
    low = pd.to_numeric(msg['k']['l'])
    high = pd.to_numeric(msg['k']['h'])
    open = pd.to_numeric(msg['k']['o'])
    volume = pd.to_numeric(msg['k']['v'])
    quoteVolume = pd.to_numeric(msg['k']['q'])
    symbol = msg['s']

    df[symbol].iloc[-1, df[symbol].columns.get_loc('Close')] = close
    df[symbol].iloc[-1, df[symbol].columns.get_loc('Low')] = low
    df[symbol].iloc[-1, df[symbol].columns.get_loc('High')] = high
    df[symbol].iloc[-1, df[symbol].columns.get_loc('Open')] = open
    df[symbol].iloc[-1, df[symbol].columns.get_loc('Volume')] = volume
    df[symbol].iloc[-1,
                    df[symbol].columns.get_loc('Quote_Volume')] = quoteVolume

    copy = DataFrame(df[symbol].iloc[-1])

    copy.to_csv(f'vwap/'+symbol+'@vwap.csv', index=True)


def printExcel():

    excel = DataFrame(columns=['symbol', 'type', 'interval', 'amount',
                      'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'total', 'closed'])

    excel.to_csv(f'results/data@vwap.csv', index=True)

    for i in exchange_pairs:
        for j in orderList[i]:

            msg = {
                'symbol': j.symbol,
                'type': j.type,
                'interval': j.interval,
                'amount': j.amount,
                'startDate': j.startDate,
                'endDate': j.endDate,
                'buy': j.buyPrice,
                'sell': j.sellPrice,
                'growth/drop': j.rate,
                'total': j.total,
                'closed': j.isSold
            }

            excel.append(msg, ignore_index=True)

    excel.to_csv(f'results/data@vwap.csv', index=True, mode='a')


def updateFrame(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['k']['s']
    close = pd.to_numeric(msg['k']['c'])
    open = pd.to_numeric(msg['k']['o'])
    check = np.where(df[symbol].iloc[-1]['Date'] == time, True, False)

    if check == True:

        updateLastRow(msg)
        vwap(len(df[symbol]) - 1, symbol)
        load_sr(symbol, open)
        checkTouch(symbol, close)
        sell(price=close, symbol=symbol, time=time)
        buy(symbol)
        printExcel()

    else:
        setBuy(symbol)
        df[symbol] = df[symbol].append({
            'Date': time,
            'Open': msg['k']['o'],
            'High': msg['k']['h'],
            'Low': msg['k']['l'],
            'Close': msg['k']['c'],
            'Volume': msg['k']['v'],
            'Quote_Volume': msg['k']['q'],

        }, ignore_index=True)
    df[symbol].iloc[-1, df[symbol].columns.get_loc('index')] = len(df[symbol])


def arrange(symbol, price):

    upper = []
    lower = []

    frame = df[symbol].iloc[-1]

    upper.append(pd.to_numeric(frame['vwap'])) if price < pd.to_numeric(
        frame['vwap']) else lower.append(pd.to_numeric(frame['vwap']))
    upper.append(pd.to_numeric(frame['sd1_pos'])) if price < pd.to_numeric(
        frame['sd1_pos']) else lower.append(pd.to_numeric(frame['sd1_pos']))
    upper.append(pd.to_numeric(frame['sd1_neg'])) if price < pd.to_numeric(
        frame['sd1_neg']) else lower.append(pd.to_numeric(frame['sd1_neg']))
    upper.append(pd.to_numeric(frame['sd2_pos'])) if price < pd.to_numeric(
        frame['sd2_pos']) else lower.append(pd.to_numeric(frame['sd2_pos']))
    upper.append(pd.to_numeric(frame['sd2_neg'])) if price < pd.to_numeric(
        frame['sd2_neg']) else lower.append(pd.to_numeric(frame['sd2_neg']))
    upper.append(pd.to_numeric(frame['sd3_pos'])) if price < pd.to_numeric(
        frame['sd3_pos']) else lower.append(pd.to_numeric(frame['sd3_pos']))
    upper.append(pd.to_numeric(frame['sd3_neg'])) if price < pd.to_numeric(
        frame['sd3_neg']) else lower.append(pd.to_numeric(frame['sd3_neg']))
    upper.append(pd.to_numeric(frame['sd4_pos'])) if price < pd.to_numeric(
        frame['sd4_pos']) else lower.append(pd.to_numeric(frame['sd4_pos']))
    upper.append(pd.to_numeric(frame['sd4_neg'])) if price < pd.to_numeric(
        frame['sd4_neg']) else lower.append(pd.to_numeric(frame['sd4_neg']))
    upper.append(pd.to_numeric(frame['sd5_pos'])) if price < pd.to_numeric(
        frame['sd5_pos']) else lower.append(pd.to_numeric(frame['sd5_pos']))
    upper.append(pd.to_numeric(frame['sd5_neg'])) if price < pd.to_numeric(
        frame['sd5_neg']) else lower.append(pd.to_numeric(frame['sd5_neg']))
    upper.append(pd.to_numeric(frame['sd6_pos'])) if price < pd.to_numeric(
        frame['sd6_pos']) else lower.append(pd.to_numeric(frame['sd6_pos']))
    upper.append(pd.to_numeric(frame['sd6_neg'])) if price < pd.to_numeric(
        frame['sd6_neg']) else lower.append(pd.to_numeric(frame['sd6_neg']))
    upper.append(pd.to_numeric(frame['sd7_pos'])) if price < pd.to_numeric(
        frame['sd7_pos']) else lower.append(pd.to_numeric(frame['sd7_pos']))
    upper.append(pd.to_numeric(frame['sd7_neg'])) if price < pd.to_numeric(
        frame['sd7_neg']) else lower.append(pd.to_numeric(frame['sd7_neg']))
    upper.append(pd.to_numeric(frame['sd8_pos'])) if price < pd.to_numeric(
        frame['sd8_pos']) else lower.append(pd.to_numeric(frame['sd8_pos']))
    upper.append(pd.to_numeric(frame['sd8_neg'])) if price < pd.to_numeric(
        frame['sd8_neg']) else lower.append(pd.to_numeric(frame['sd8_neg']))

    upper = np.sort(upper)
    lower = np.sort(lower)
    lower = lower[::-1]

    return upper, lower


def load_sr(symbol, price):

    try:
        if preOrder[symbol].isSet == False:

            upper, lower = arrange(symbol, price)

            preOrder[symbol].head.value = pd.to_numeric(price)

            preOrder[symbol].isSet = True

            preOrder[symbol].head.next = Node(value=upper[0])
            preOrder[symbol].head.before = Node(value=lower[0])

        else:

            upper, lower = arrange(symbol, preOrder[symbol].head.value)

            preOrder[symbol].head.next = Node(value=upper[0])
            preOrder[symbol].head.before = Node(value=lower[0])

    except Exception as e:

        print('error while loading ' + symbol)

        print(e)


def init():
    for pair in exchange_pairs:
        preOrder[pair] = Order()
        orderList[pair] = []


def checkTouch(symbol, price):

    try:
        level = preOrder[symbol].head.next.value + \
            (preOrder[symbol].head.next.value * 0.02)
        if (preOrder[symbol].isBuy == True) and (price <= level):
            preOrder[symbol].isTouch = True
    except:

        print('')


def setBuy(symbol):

    try:

        close = df[symbol].iloc[-1, df[symbol].columns.get_loc('Close')]

        if close > preOrder[symbol].head.next.value and preOrder[symbol].isBuy != True:

            preOrder[symbol].isBuy = True
            send_message('--- Cross Vwap ---\nDate : ' + str(df[symbol].iloc[-1, df[symbol].columns.get_loc(
                'Date')]) + '\nSymbol : ' + str(symbol) + '\nPrice : ' + str(close) + '\nNext Vwap : ' + str(preOrder[symbol].head.next.value) + '\nBefore Vwap : ' + str(preOrder[symbol].head.before.value))

        elif close < preOrder[symbol].head.before.value and preOrder[symbol].isBuy != True:

            preOrder[symbol].isBuy = False
            preOrder[symbol].isTouch = False
            preOrder[symbol].isSet = False
            preOrder[symbol].isOrder = False

    except Exception as e:
        print(e)
        print('Error caused by set buy... ' + str(symbol))


def buy(symbol):

    # try:
    close = df[symbol].iloc[-1, df[symbol].columns.get_loc('Close')]
    date = str(df[symbol].iloc[-1, df[symbol].columns.get_loc('Date')])

    if preOrder[symbol].isOrder != True:

        if preOrder[symbol].isBuy == True and preOrder[symbol].isTouch == True and close >= preOrder[symbol].head.next.value:

            send_message('--- Buy ---\nDate : ' + str(date) +
                         '\nSymbol : ' + str(symbol) + '\nPrice : ' + str(close))

            preOrder[symbol].isOrder = True
            orderList[symbol].append(Book(
                type='vwap', symbol=symbol, interval='2h', amount=500.0, buyPrice=close, startDate=date))

        elif preOrder[symbol].isBuy == True and preOrder[symbol].isTouch == True and close < preOrder[symbol].head.next.value:

            preOrder[symbol].isBuy = False
            preOrder[symbol].isTouch = False
            preOrder[symbol].isSet = False
            preOrder[symbol].isOrder = False

    # except:

    #     print('Error caused by buy... ' + str(symbol))


def sell(symbol, price, time):

    try:

        for el in orderList[symbol]:

            rate = ((price - el.buyPrice) / el.buyPrice) * 100

            if el.isSold == False:

                el.sellPrice = price
                el.endDate = time
                el.rate = rate
                el.total += el.buyPrice * el.rate

                if rate >= 5.0:

                    el.isSold = True
                    send_message('--- Sell ---\n' + 'Symbol : ' + str(symbol) + '\nBuy Date : ' + str(el.startDate) + '\nSell Date : '+str(
                        time) + '\nBuy Price : ' + str(el.buyPrice) + '\nSell Price : ' + str(price) + '\nProfit ' + str(rate) + '%')

                elif rate <= -5.0:

                    el.isSold = True
                    send_message('--- Stoplose ---\n' + 'Symbol : ' + str(symbol) + '\nBuy Date : ' + str(el.startDate) + '\nSell Date : '+str(
                        time) + '\nBuy Price : ' + str(el.buyPrice) + '\nSell Price : ' + str(price) + '\nDrop ' + str(rate) + '%')

    except Exception as e:

        print('Error caused by selling... ' + str(symbol))
        print(e)


def handle_socket_message(msg):

    # 1 - load prev orders.
    # 2 - load pairs SR and set them.

    updateFrame(msg)


def realtime(msg):
    if 'data' in msg:
        # Your code
        handle_socket_message(msg['data'])

    else:
        stream.stream_error = True


init()
for pair in exchange_pairs:
    history(symbol=pair)
    print(pair + '  history is loaded.')

print('Done...')
print('Start streaming...')


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@kline_2h')
        self.multiplex = self.bm.start_multiplex_socket(
            callback=realtime, streams=self.multiplex_list)

        # monitoring the error
        stop_trades = threading.Thread(
            target=stream.restart_stream, daemon=True)
        stop_trades.start()

    def restart_stream(self):
        while True:
            time.sleep(1)
            if self.stream_error == True:
                self.bm.stop_socket(self.multiplex)
                time.sleep(5)
                self.stream_error = False
                self.multiplex = self.bm.start_multiplex_socket(
                    callback=realtime, streams=self.multiplex_list)


stream = Stream()
stream.start()
stream.bm.join()
