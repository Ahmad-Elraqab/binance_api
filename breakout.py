from os import close
from tabnanny import check
import threading
import time
from models.order import Order
from matplotlib import pyplot as plt
from binance import Client
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import AsyncClient, Client
from datetime import date, datetime
from binance.streams import ThreadedWebsocketManager
import numpy as np
import pandas as pd
import numpy as np
import csv
from client import send_message

points_list = {}
kilne_tracker = {}

client = Client(api_key=API_KEY, api_secret=API_SECRET)
tickers = client.get_all_tickers()


def isResistance(df, i):

    try:
        resistance = float(df[i][2]) > float(df[i-1][2]) and float(df[i][2]) > float(df[i+1][2]) \
            and float(df[i+1][2]) > float(df[i+2][2]) and float(df[i-1][2]) > float(df[i-2][2])

        return resistance

    except:

        print("Invalid Index")


def getData(interval):

    global points_list

    for index, pair in enumerate(exchange_pairs):

        try:
            klines = client.get_historical_klines(
                pair, getInterval(interval), "3 month ago")

            df = pd.DataFrame(klines)
            # df[0] = pd.to_datetime(df[0], unit='ms')

            df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                          'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

            df = df.drop(columns=['IGNORE',
                                  'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

            # df = df.set_index('Date')
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

            list = []

            for i, value in enumerate(klines):

                if isResistance(klines, i):
                    h = klines[i][2]
                    list.append(
                        {'s': pair, 'h': pd.to_numeric(h), 'd': pd.to_datetime(klines[i][0], unit='ms')})

            analyzePoint(pair, list)

            kilne_tracker[pair] = df

        except:
            print(i)


def analyzePoint(symbol, list):

    global points_list
    points_list[symbol] = {}
    print(symbol)
    print(list[-1]['d'])
    print(list[-1]['h'])
    print(list[-2]['d'])
    print(list[-2]['h'])
    print('lmao')
    if len(list) >= 2:
        print('lmao')

        if list[-1]['h'] < list[-2]['h']:
            print('lmao')

            difference = (list[-1]['d'] - list[-2]['d'])
            total_seconds = difference.total_seconds()

            hours = divmod(total_seconds, 14400)[0]
            points_list[symbol]['status'] = False
            points_list[symbol]['isCross'] = False
            points_list[symbol]['value'] = (
                list[-1]['h']-list[-2]['h']) / hours
            points_list[symbol]['d'] = list[-2]['d']
            points_list[symbol]['h'] = list[-2]['h']
            points_list[symbol]['h1'] = list[-1]['h']
            points_list[symbol]['d1'] = list[-1]['d']


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


def handle_socket_message(msg):
    # try:
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']
    price = msg['k']['h']

    check = np.where(
        kilne_tracker[symbol].iloc[-1]['Date'] == time, True, False)

    if check == True:

        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Open')] = float(msg['k']['o'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('High')] = float(msg['k']['h'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Low')] = float(msg['k']['l'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Close')] = float(msg['k']['c'])
        kilne_tracker[symbol].iloc[-1,
                                   kilne_tracker[symbol].columns.get_loc('Volume')] = float(msg['k']['v'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol]
                                   .columns.get_loc('Quote_Volume')] = float(msg['k']['q'])

        if len(points_list[symbol]) != 0:
            if points_list[symbol]['status'] == False and pd.to_numeric(price) <= pd.to_numeric(points_list[symbol]['limit']):
                points_list[symbol]['status'] = True
                send_message('4H Track ----- ' + str(symbol) + '\n' +
                             str(points_list[symbol]['d']) + ' -- ' + str(points_list[symbol]['h']) + '\n' +
                             str(points_list[symbol]['d1']) + ' -- ' + str(points_list[symbol]['h1']) + '\n' +
                             'M -- ' + str(points_list[symbol]['value']) + '\n' +
                             str(points_list[symbol]['limit']) + ' -- ' + str(points_list[symbol]['num']) + '\n' +
                             str("Fine, I'll Do It Myself..."),
                             '-720702466')

            elif points_list[symbol]['status'] == True and pd.to_numeric(price) >= pd.to_numeric(points_list[symbol]['limit']) and points_list[symbol]['isCross'] == False:
                points_list[symbol]['isCross'] = True
                send_message('4H Track ----- ' + str(symbol) + '\n' +
                             str(points_list[symbol]['d']) + ' -- ' + str(points_list[symbol]['h']) + '\n' +
                             str(points_list[symbol]['d1']) + ' -- ' + str(points_list[symbol]['h1']) + '\n' +
                             'M -- ' + str(points_list[symbol]['value']) + '\n' +
                             str(points_list[symbol]['limit']) + ' -- ' + str(points_list[symbol]['num']) + '\n' +
                             str('Fly Me To The Moon...'),
                             '-720702466')

    else:

        kilne_tracker[symbol] = kilne_tracker[symbol].append({
            'Date': time,
            'Open': msg['k']['o'],
            'High': msg['k']['h'],
            'Low': msg['k']['l'],
            'Close': msg['k']['c'],
            'Volume': msg['k']['v'],
            'Quote_Volume': msg['k']['q'],
        }, ignore_index=True)

        checkTouch(symbol, time)


def checkTouch(symbol, date):

    if len(points_list[symbol]) != 0:

        difference = (date - points_list[symbol]['d'])
        total_seconds = difference.total_seconds()

        hours = divmod(total_seconds, 14400)[0]

        points_list[symbol]['limit'] = points_list[symbol]['h'] + \
            (points_list[symbol]['value'] * hours)

        points_list[symbol]['num'] = hours


def realtime(msg):
    if 'data' in msg:
        # Your code
        handle_socket_message(msg['data'])

    else:
        stream.stream_error = True


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
            self.multiplex_list.append(pairing.lower() + '@kline_4h')
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


getData('4hr')


stream = Stream()
stream.start()
stream.bm.join()
