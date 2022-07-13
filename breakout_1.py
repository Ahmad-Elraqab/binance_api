from os import close
from tabnanny import check
import threading
import time
import uuid
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
import emoji
import csv
from client import send_message
import sys
import os
import concurrent.futures

points_list = {}
kilne_tracker = {}

client = Client(api_key=API_KEY, api_secret=API_SECRET)
tickers = client.get_all_tickers()

excel_df = pd.DataFrame(columns=['id', 'symbol', 'amount', 'startDate',
                        'endDate', 'buy', 'sell', 'growth/drop', 'closed', 'diff', 'h1', 'h2'])
orders = []

INTERVAL = '4h'
DATETIME = '1 Mar 2022'
SECOND = 14400
FILENAME = 'data@break_10.csv'
SELLLIMIT = 10.5
STOPLOSE = -5.0
BUYLIMIT = 0
# TELEGRAMID = '-720702466'
TELEGRAMID = '-753037876'


class Order:
    def __init__(self, id, symbol, buyPrice, amount, startDate, diff):

        self.id = id
        self.symbol = symbol
        self.buyPrice = buyPrice
        self.sellPrice = None
        self.amount = amount
        self.startDate = startDate
        self.endDate = startDate
        self.rate = None
        self.isSold = False
        self.diff = diff


def isResistance(df, i):

    try:
        resistance = float(df[i][2]) > float(df[i-1][2]) and float(df[i][2]) > float(df[i+1][2]) \
            and float(df[i+1][2]) > float(df[i+2][2]) and float(df[i-1][2]) > float(df[i-2][2])

        return resistance

    except:

        # print("Invalid Index")
        pass


def isResistance_2(df, i):

    try:
        resistance = float(df.iloc[i, df.columns.get_loc('High')]) > float(df.iloc[i-1, df.columns.get_loc('High')]) and float(df.iloc[i, df.columns.get_loc('High')]) > float(df.iloc[i+1, df.columns.get_loc('High')]) \
            and float(df.iloc[i+1, df.columns.get_loc('High')]) > float(df.iloc[i+2, df.columns.get_loc('High')]) and float(df.iloc[i-1, df.columns.get_loc('High')]) > float(df.iloc[i-2, df.columns.get_loc('High')])

        return resistance

    except:

        # print("Invalid Index")
        pass


def getData(pair):

    try:
        klines = client.get_historical_klines(
            pair, getInterval(INTERVAL), start_str=DATETIME)

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

            if isResistance(klines, i) == True:
                h = klines[i][2]
                list.append(
                    {'s': pair, 'h': pd.to_numeric(h), 'd': pd.to_datetime(klines[i][0], unit='ms')})

        kilne_tracker[pair] = df
        points_list[pair] = {}
        points_list[pair]['status'] = False
        points_list[pair]['isCross'] = False
        points_list[pair]['value'] = 0
        points_list[pair]['d'] = 0
        points_list[pair]['h'] = 0
        points_list[pair]['h1'] = 0
        points_list[pair]['d1'] = 0
        points_list[pair]['check'] = False
        points_list[pair]['limit'] = 0

        analyzePoint(pair, list)
        print('done')
    except:
        # print(i)
        pass


def double_check(symbol):

    list = []

    for i, value in kilne_tracker[symbol].iterrows():

        if isResistance_2(kilne_tracker[symbol], i) == True:
            h = kilne_tracker[symbol].iloc[i,
                                           kilne_tracker[symbol].columns.get_loc('High')]
            list.append(
                {'s': symbol, 'h': pd.to_numeric(h), 'd': pd.to_datetime(kilne_tracker[symbol].iloc[i, kilne_tracker[symbol].columns.get_loc('Date')], unit='ms')})

    analyzePoint(symbol, list)


def analyzePoint(symbol, list):

    try:
        filtered_arr = [p for p in list if p['h'] >= pd.to_numeric(
            kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Close')])]

        global points_list

        if len(filtered_arr) >= 2:

            for i in reversed(filtered_arr):

                if filtered_arr[-1]['h'] < i['h']:

                    difference_1 = (pd.to_datetime(kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc(
                        'Date')], unit='ms') - pd.to_datetime(i['d'], unit='ms'))
                    total_seconds_1 = difference_1.total_seconds()
                    hours_1 = divmod(total_seconds_1, SECOND)[0]

                    highest = kilne_tracker[symbol]['High'].rolling(
                        int(hours_1)).max()

                    if i['h'] > pd.to_numeric(highest[len(highest)-1]):

                        difference = (filtered_arr[-1]['d'] - i['d'])
                        total_seconds = difference.total_seconds()

                        hours = divmod(total_seconds, SECOND)[0]
                        points_list[symbol]['status'] = False
                        points_list[symbol]['isCross'] = False
                        points_list[symbol]['value'] = (
                            filtered_arr[-1]['h']-i['h']) / hours

                        send_message(str(symbol) + '\nnew peak\n' +
                                     str(filtered_arr[-1]['h']) + '\n' + str(i['h']), TELEGRAMID)

                        if points_list[symbol]['check'] == True:
                            if points_list[symbol]['h'] == i['h'] and points_list[symbol]['h1'] == filtered_arr[-1]['h']:

                                print('hi')
                            else:
                                send_message(
                                    'Change 1h' + str(symbol) + '\n' + 'old peak\n' + str(points_list[symbol]['h1']) + '\n' + str(points_list[symbol]['h']) + '\nnew peak\n' + str(filtered_arr[-1]['h']) + '\n' + str(i['h']), TELEGRAMID)

                        points_list[symbol]['d'] = i['d']
                        points_list[symbol]['h'] = i['h']
                        points_list[symbol]['h1'] = filtered_arr[-1]['h']
                        points_list[symbol]['d1'] = filtered_arr[-1]['d']
                        print('---------------------------')
                        print(symbol)
                        print(points_list[symbol]['h'])
                        print(points_list[symbol]['h1'])
                        print(highest)
                        print('---------------------------')
                        highest_1 = kilne_tracker[symbol]['Volume'].rolling(
                            int(hours)).mean()

                        # if i['h'] > pd.to_numeric(highest[len(highest)-1]):

                        points_list[symbol]['check'] = True
                        break

    except Exception as e:
        print(e)


def getInterval(interval):
    if interval == "1D":
        return Client.KLINE_INTERVAL_1DAY
    elif interval == "12h":
        return Client.KLINE_INTERVAL_12HOUR
    elif interval == "8h":
        return Client.KLINE_INTERVAL_8HOUR
    elif interval == "6h":
        return Client.KLINE_INTERVAL_6HOUR
    elif interval == "4h":
        return Client.KLINE_INTERVAL_4HOUR
    elif interval == "1h":
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
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']
    price = pd.to_numeric(msg['k']['h'])
    b_price = pd.to_numeric(msg['k']['c'])
    global orders
    global excel_df

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

        if points_list[symbol]['status'] == False and pd.to_numeric(price) <= pd.to_numeric(points_list[symbol]['limit']):
            points_list[symbol]['status'] = True

        elif points_list[symbol]['status'] == True and points_list[symbol]['isCross'] == False:
            value = (pd.to_numeric(price) - pd.to_numeric(
                points_list[symbol]['limit'])) / pd.to_numeric(points_list[symbol]['limit']) * 100

            if value >= BUYLIMIT:
                points_list[symbol]['isCross'] = True
                points_list[symbol]['price'] = b_price

                send_message('4H Track ----- ' + str(symbol) + '\n' +
                             'peak 1 ' + str(points_list[symbol]['d']) + ' -- ' + str(points_list[symbol]['h']) + '\n' +
                             'peak 2 ' + str(points_list[symbol]['d1']) + ' -- ' + str(points_list[symbol]['h1']) + '\n' +
                             'avg peak ' + str((points_list[symbol]['h'] - points_list[symbol]['h1']) / points_list[symbol]['h1'] * 100) + '\n' +
                             'M -- ' + str(points_list[symbol]['value']) + '\n' +
                             'Limit ' + str(points_list[symbol]['limit']) + ' -- ' + str(points_list[symbol]['num']) + '\n' +
                             'candle ch ' + str((pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Close']) - pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Open'])) / pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Open']) * 100) + '\n' +
                             'candle v ' + str(kilne_tracker[symbol].iloc[-1]['Volume']) + '\n' +
                             'avg Limit ' + str((b_price - points_list[symbol]['limit']) / points_list[symbol]['limit'] * 100) + '\n' +
                             'Buy ' + str(b_price) + ' -- ' + str(time) + '\n' +
                             str("Buy, Fine, I'll Do It Myself..." +
                                 emoji.emojize(string='joker')),
                             TELEGRAMID)

                order = Order(
                    id=uuid.uuid1(),
                    symbol=symbol,
                    buyPrice=kilne_tracker[symbol].iloc[-1]['Close'],
                    amount=500,
                    startDate=time,
                    diff=(points_list[symbol]['h'] - points_list[symbol]
                          ['h1']) / points_list[symbol]['h1'] * 100
                )
                orders.append(order)

                msg_ = {
                    'id': order.id,
                    'symbol': order.symbol,
                    'amount': order.amount,
                    'startDate': order.startDate,
                    'endDate': order.endDate,
                    'buy': order.buyPrice,
                    'sell': order.sellPrice,
                    'closed': order.isSold,
                    'growth/drop': order.rate,
                    'h1': points_list[symbol]['h'],
                    'h2': points_list[symbol]['h1'],
                    'd1': points_list[symbol]['d'],
                    'd2': points_list[symbol]['d1'],
                    'diff': (points_list[symbol]['h'] - points_list[symbol]['h1']) / points_list[symbol]['h1'] * 100,
                    'candle ch': (pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Close']) - pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Open'])) / pd.to_numeric(kilne_tracker[symbol].iloc[-1]['Open']) * 100,
                    'candle v': kilne_tracker[symbol].iloc[-1]['Volume']
                }

                excel_df = excel_df.append(msg_, ignore_index=True)

                excel_df.to_csv(f'results/'+FILENAME, header=True)

        elif points_list[symbol]['isCross'] == True:

            for order in orders:

                if order.isSold == False and order.symbol == symbol:

                    rate = ((float(price) - float(order.buyPrice)) /
                            float(order.buyPrice)) * 100

                    order.rate = rate
                    order.endDate = time

                    if rate >= SELLLIMIT:

                        order.isSold = True
                        points_list[symbol]['isCross'] = False
                        points_list[symbol]['status'] = False

                        send_message('4H Track ----- ' + str(symbol) + '\n' + 'peak 1' + str(points_list[symbol]['d']) + ' -- ' + str(points_list[symbol]['h']) + '\n' + 'peak 2' + str(points_list[symbol]['d1']) + ' -- ' + str(points_list[symbol]['h1']) + '\n' + 'M -- ' + str(points_list[symbol]['value']) + '\n' + 'Limit' + str(
                            points_list[symbol]['limit']) + ' -- ' + str(points_list[symbol]['num']) + '\n' + 'BUY' + str(points_list[symbol]['price']) + ' -- ' + 'SEll' + str(b_price) + ' -- ' + str(time) + '\n' + str("Sell, By Order Of The Peaky Fucking Blinders..." + emoji.emojize(string='rocket')), TELEGRAMID)

                    elif rate <= STOPLOSE:
                        order.isSold = True
                        points_list[symbol]['isCross'] = False
                        points_list[symbol]['status'] = False

                        send_message('4H Track STOPLOSE ' + str(symbol) + '\n' + 'peak 1' + str(points_list[symbol]['d']) + ' -- ' + str(points_list[symbol]['h']) + '\n' + 'peak 2' + str(points_list[symbol]['d1']) + ' -- ' + str(points_list[symbol]['h1']) + '\n' + 'M -- ' + str(points_list[symbol]['value']) + '\n' + 'Limit' + str(
                            points_list[symbol]['limit']) + ' -- ' + str(points_list[symbol]['num']) + '\n' + 'BUY' + str(points_list[symbol]['price']) + ' -- ' + 'SEll' + str(b_price) + ' -- ' + str(time) + '\n' + str("Sell, By Order Of The Peaky Fucking Blinders..." + emoji.emojize(string='rocket')), TELEGRAMID)

                    excel_df.loc[excel_df['id'] ==
                                 order.id, 'sell'] = b_price
                    excel_df.loc[excel_df['id'] ==
                                 order.id, 'endDate'] = time
                    excel_df.loc[excel_df['id'] ==
                                 order.id, 'closed'] = order.isSold
                    excel_df.loc[excel_df['id'] == order.id,
                                 'growth/drop'] = order.rate

            excel_df.to_csv(f'results/'+FILENAME, header=True)

    else:

        if points_list[symbol]['isCross'] == False and points_list[symbol]['status'] == False:

            # print(points_list)
            double_check(symbol=symbol)

        checkTouch(symbol, time)

        kilne_tracker[symbol] = kilne_tracker[symbol].append({
            'Date': time,
            'Open': msg['k']['o'],
            'High': msg['k']['h'],
            'Low': msg['k']['l'],
            'Close': msg['k']['c'],
            'Volume': msg['k']['v'],
            'Quote_Volume': msg['k']['q'],
        }, ignore_index=True)


def checkTouch(symbol, date):

    try:
        difference = (date - points_list[symbol]['d'])
        total_seconds = difference.total_seconds()

        hours = divmod(total_seconds, SECOND)[0]

        points_list[symbol]['limit'] = points_list[symbol]['h'] + (points_list[symbol]['value'] * hours)

        points_list[symbol]['num'] = hours
    except:
        pass


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
            self.multiplex_list.append(pairing.lower() + '@kline_' + INTERVAL)
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


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(getData, exchange_pairs)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} seconds')

print('NEW')

stream = Stream()
stream.start()
stream.bm.join()