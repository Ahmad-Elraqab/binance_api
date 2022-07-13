import os
from sqlite3 import Timestamp
import sys
from audioop import reverse
import threading
import time
from unittest import case
import uuid
from binance import Client
import numpy
import pandas
from pytz import HOUR
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import concurrent.futures
from binance.client import AsyncClient, Client
from datetime import date, datetime
from binance.streams import ThreadedWebsocketManager
import keyboard
import csv

client = Client(api_key=API_KEY, api_secret=API_SECRET)

points = {}
orders = []

excel_df = pandas.DataFrame(columns=[
    'id', 'symbol', 'startDate', 'DIFF', 'SDIFF', 'endDate', 'buy', 'rate', 'closed'])

points_list = {}

_date = datetime.now()
utc_time = time.mktime(_date.timetuple())
new_time = pandas.to_datetime(utc_time, unit='ms')


class Order:
    def __init__(self, id, diff, symbol, buyPrice, startDate):

        self.id = id
        self.symbol = symbol
        self.buyPrice = buyPrice
        self.startDate = startDate
        self.endDate = startDate
        self.rate = None
        self.diff = diff
        self.isSold = False


def mul_1(i):
    return i['true']


def mul_2(i):
    return i['false']


def getData(symbol):

    tickers = client.aggregate_trade_iter(
        symbol=symbol, start_str='1 hour ago')

    data = pandas.DataFrame(tickers)
    data.columns = ['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M']
    data['T'] = pandas.to_datetime(data['T'], unit='ms')

    column = pandas.to_datetime(
        data.iloc[-1, data.columns.get_loc('T')], unit='ms')

    day = int(column.day)
    hour = int(column.hour)
    minute = int(column.minute)

    points_list[symbol] = False
    points[symbol] = {}
    points[symbol] = {
        day: {
            hour: {
                'SC': 0,
                'SA': 0,
                'BA': 0,
                'BD': 0,
                'SD': 0,
                'BC': 0,
                'DIFF': 0,
                'V': 0,
            },
        },
    }

    for index, msg in data.iterrows():
        time = pandas.to_datetime(msg['T'], unit='ms')
        qty = pandas.to_numeric(msg['q'])
        type = msg['m']
        price = pandas.to_numeric(msg['p'])
        value = qty * price

        day = time.day
        hour = time.hour
        minute = time.minute

        if day in points[symbol].keys():

            if hour in points[symbol][day].keys():

                if type == True:
                    points[symbol][day][hour]['SC'] += 1
                    points[symbol][day][hour]['SD'] += value
                    points[symbol][day][hour]['SA'] = points[symbol][day][hour]['SD'] / \
                        points[symbol][day][hour]['SC']
                else:
                    points[symbol][day][hour]['BC'] += 1
                    points[symbol][day][hour]['BD'] += value
                    points[symbol][day][hour]['BA'] = points[symbol][day][hour]['BD'] / \
                        points[symbol][day][hour]['BC']

                points[symbol][day][hour]['V'] += value
                points[symbol][day][hour]['DIFF'] = points[symbol][day][hour]['BD'] - \
                    points[symbol][day][hour]['SD']

            else:

                points[symbol][day][hour] = {
                    'SC': 0,
                    'SA': 0,
                    'BA': 0,
                    'BD': 0,
                    'SD': 0,
                    'BC': 0,
                    'DIFF': 0,
                    'V': 0,
                }

        else:

            points[symbol][day] = {
                hour: {
                    'SC': 0,
                    'SA': 0,
                    'BA': 0,
                    'BD': 0,
                    'SD': 0,
                    'BC': 0,
                    'V': 0,
                }
            }


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(getData, exchange_pairs)

t2 = time.perf_counter()

print('NEW...')


def realtime(msg):

    if 'data' in msg:

        if '@miniTicker' in msg['stream']:
            sell(msg['data']['s'], pandas.to_numeric(msg['data']['c']),
                 pandas.to_datetime(msg['data']['E'], unit='ms'))

        elif '@aggTrade' in msg['stream']:

            handle_socket_message(msg['data'])
    else:
        stream.stream_error = True


def handle_socket_message(msg):

    time = pandas.to_datetime(msg['T'], unit='ms')
    qty = pandas.to_numeric(msg['q'])
    price = pandas.to_numeric(msg['p'])
    symbol = msg['s']
    type = msg['m']
    value = qty * price

    day = time.day
    hour = time.hour
    minute = time.minute
    global points

    if day in points[symbol].keys():

        if hour in points[symbol][day].keys():

            if type == True:
                points[symbol][day][hour]['SC'] += 1
                points[symbol][day][hour]['SD'] += value
                points[symbol][day][hour]['SA'] = points[symbol][day][hour]['SD'] / \
                    points[symbol][day][hour]['SC']
            else:
                points[symbol][day][hour]['BC'] += 1
                points[symbol][day][hour]['BD'] += value
                points[symbol][day][hour]['BA'] = points[symbol][day][hour]['BD'] / \
                    points[symbol][day][hour]['BC']

            points[symbol][day][hour]['V'] += value
            points[symbol][day][hour]['DIFF'] = points[symbol][day][hour]['BD'] - \
                points[symbol][day][hour]['SD']

            printTop(time, msg['T'])
        else:

            buy(symbol=symbol, time=time, price=price,
                diff=points[symbol][day][hour-1]['DIFF'])

            points[symbol][day][hour] = {
                'SC': 0,
                'SA': 0,
                'BA': 0,
                'BD': 0,
                'SD': 0,
                'BC': 0,
                'DIFF': 0,
                'V': 0,
            }
    else:

        points[symbol][day] = {
            hour: {
                'SC': 0,
                'SA': 0,
                'BA': 0,
                'BD': 0,
                'SD': 0,
                'BC': 0,
                'DIFF': 0,
                'V': 0,
            }
        }


def printTop(time, _t):

    global new_time
    difference = (time - new_time)
    total_seconds = difference.total_seconds()

    hours = divmod(total_seconds, 60)[0]

    if float(hours) >= 60:

        new_time = time
        global points

        list = []
        for key, value in points.items():
            list.append(
                {'key': key, 'value': value[time.day][time.hour-1]['DIFF']})

        newlist = sorted(list, key=lambda x: x['value'], reverse=True)

        msg = '\t\tTOP 10 COINS\n\n'

        for i in newlist[0:10]:

            msg = msg + (i['key'] + ' --- ' +
                         str(numpy.round(int(i['value']), 2)) + '\n')

        send_message(str(msg), '-720702466')


def buy(symbol, price, time, diff):

    global orders
    global excel_df
    if points_list[symbol] == False:

        if (diff) >= 500000:

            points_list[symbol] = True
            order = Order(
                id=uuid.uuid1(),
                symbol=symbol,
                buyPrice=price,
                startDate=time,
                diff=diff,
            )
            orders.append(order)

            msg_ = {
                'id': order.id,
                'symbol': order.symbol,
                'startDate': order.startDate,
                'endDate': order.endDate,
                'buy': order.buyPrice,
                'DIFF': diff,
                'closed': order.isSold,
            }

            send_message('BUY - SYMBOL : ' + str(symbol) + '\n' +
                         'PRICE : ' + str(price) + '\n' + 'DATE : ' + str(time) + '\n' + 'DIFF : ' + str(diff), '-720702466')

            excel_df = excel_df.append(msg_, ignore_index=True)

            excel_df.to_csv(f'results/dataset_3.csv', header=True)


def sell(symbol, price, time):

    global orders
    global excel_df

    for order in orders:

        if order.isSold == False and order.symbol == symbol:

            rate = (price - order.buyPrice) / order.buyPrice * 100

            if (rate >= 2.0):

                points_list[symbol] = False
                order.isSold = True

                send_message('Sell - SYMBOL : ' + str(symbol) + '\n' +
                             'PRICE : ' + str(price) + '\n' + 'DATE : ' + str(time) + '\n' + 'DIFF : ' + str(points[symbol][time.day][time.hour-1]['DIFF']), '-720702466')

            order.endDate = time
            order.rate = rate
            excel_df.loc[excel_df['id'] == order.id, 'rate'] = rate
            excel_df.loc[excel_df['id'] == order.id, 'endDate'] = time
            excel_df.loc[excel_df['id'] == order.id,
                         'SDIFF'] = points[symbol][time.day][time.hour-1]['DIFF']
            excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold

            excel_df.to_csv(f'results/dataset_3.csv', header=True)


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@aggTrade')
            self.multiplex_list.append(pairing.lower() + '@miniTicker')
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
                time.sleep(1)
                self.stream_error = False
                self.multiplex = self.bm.start_multiplex_socket(
                    callback=realtime, streams=self.multiplex_list)


stream = Stream()
stream.start()
stream.bm.join()
