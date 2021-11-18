from os import close
import threading
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
from client import send_message

# import xlsxwriter
# import mplfinance as mpf


points_list = {}

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

    for index, pair in enumerate(exchange_pairs):
        klines = client.get_historical_klines(
            pair, getInterval(interval), "3 days ago")

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

        list = []

        for i, value in enumerate(klines):

            if isResistance(klines, i):
                h = klines[i][2]
                list.append(pd.to_numeric(h))

        check = analyzePoint(pair, list)

        points_list[pair] = {}
        points_list[pair]['isCheck'] = False
        points_list[pair]['value'] = 0

        if check == 0:
            points_list[pair]['check'] = False
        else:
            points_list[pair]['check'] = True
            points_list[pair]['value'] = check

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


def analyzePoint(symbol, list):

    if len(list) >= 3:

        if list[-1] < list[-2] and list[-2] < list[-3]:

            points_list[symbol] = list[-1]

            send_message('4h Track ----- ' + str(symbol) + '\n levels:\n' +
                         str(list[-1]) + '\n' + str(list[-2]) + '\n' + str(list[-3]))

            return list[-1]

        else:
            return 0

    else:
        return 0


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

    # 1 - load prev orders.
    # 2 - load pairs SR and set them.
    symbol = msg['k']['s']
    price = msg['k']['c']

    if points_list[symbol]['check'] == True:
        if pd.to_numeric(points_list[symbol]['value']) <= pd.to_numeric(price) and points_list[symbol]['isCheck'] == False:
            send_message(symbol + ' 4h alert breakout level...' +
                         ' at ' + str(points_list[symbol]['value']))
            points_list[symbol]['isCheck'] = True


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
