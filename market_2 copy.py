import math
import threading
import time
from unittest import case
import uuid
from binance import Client
import numpy
import numpy as np
import pandas
import pandas as pd
from pytz import HOUR
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import concurrent.futures
from binance.client import AsyncClient, Client
from datetime import date, datetime
from binance.streams import ThreadedWebsocketManager
import csv
import sys
import os

client = Client(api_key=API_KEY, api_secret=API_SECRET)

points = {}
orders = []
kilne_tracker = {}
kilne_status = {}
points_list = {}

excel_df = pandas.DataFrame(columns=['Id', 'Symbol', 'Date', 'Price',
                            'BC', 'SC', 'TPS', 'Volume', 'DIFF', 'ZSCORE', 'High', 'Low', 'Close', 'sell', 'sold', 's-date'])


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


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = numpy.sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


def updateFrame(symbol, msg):

    # try:

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

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

    setDatafFame(symbol=symbol)


def setDatafFame(symbol):

    global kilne_tracker
    close = pd.to_numeric(kilne_tracker[symbol]['Close'])

    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    temps = zScore(
        window=48, close=close, volume=volume)

    kilne_tracker[symbol]['48-zscore'] = temps


def readHistory(i):

    print(i)
    global kilne_tracker

    klines = client.get_historical_klines(
        symbol=i, interval=Client.KLINE_INTERVAL_1MINUTE, start_str='2 hours ago UTC')

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')
    data['Volume'] = pd.to_numeric(
        data['Volume'], errors='coerce')
    data['High'] = pd.to_numeric(
        data['High'], errors='coerce')
    data['Low'] = pd.to_numeric(
        data['Low'], errors='coerce')

    data['48-zscore'] = 0

    kilne_tracker[i] = data
    kilne_status[i] = False

    setDatafFame(i)


def getData(symbol):

    try:

        agg_trades = client.aggregate_trade_iter(
            symbol=symbol, start_str='1 hours ago UTC')

        data = pandas.DataFrame(agg_trades)
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
                    minute: {
                        'SC': 0,
                        'SA': 0,
                        'BA': 0,
                        'BD': 0,
                        'SD': 0,
                        'BC': 0,
                        'DIFF': 0,
                        'V': 0,
                    }
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

                    if minute in points[symbol][day][hour].keys():

                        if type == True:
                            points[symbol][day][hour][minute]['SC'] += 1
                            points[symbol][day][hour][minute]['SD'] += value
                            points[symbol][day][hour][minute]['SA'] = points[symbol][day][hour][minute]['SD'] / \
                                points[symbol][day][hour][minute]['SC']
                        else:
                            points[symbol][day][hour][minute]['BC'] += 1
                            points[symbol][day][hour][minute]['BD'] += value
                            points[symbol][day][hour][minute]['BA'] = points[symbol][day][hour][minute]['BD'] / \
                                points[symbol][day][hour][minute]['BC']

                        points[symbol][day][hour][minute]['V'] += value
                        points[symbol][day][hour][minute]['DIFF'] = points[symbol][day][hour][minute]['BD'] - \
                            points[symbol][day][hour][minute]['SD']

                    else:

                        points[symbol][day][hour][minute] = {
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

                    points[symbol][day][hour] = {

                    }

            else:

                points[symbol][day] = {

                }

    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        print(symbol)


def realtime(msg):

    if 'data' in msg:

        symbol = msg['data']['s']

        # print(kilne_tracker)
        if '@kline_1m' in msg['stream']:

            updateFrame(symbol=symbol, msg=msg['data'])
            time = pd.to_datetime(msg['data']['k']['t'], unit='ms')
            price = kilne_tracker[symbol].iloc[-1,
                                               kilne_tracker[symbol].columns.get_loc('Close')]

            global excel_df

            excel_df.loc[excel_df['Symbol'] == symbol, 'Close'] = price

            t_data = excel_df.loc[excel_df['Symbol'] == symbol]

            for i, value in t_data.iterrows():

                rate = ((float(price) - float(value['Price'])) /
                        float(value['Price'])) * 100

                zscore = kilne_tracker[symbol].iloc[-1,
                                                    kilne_tracker[symbol].columns.get_loc('48-zscore')]

                if pd.to_numeric(price) > pd.to_numeric(value['High']):

                    excel_df.loc[excel_df['Id'] == value['Id'], 'High'] = price

                elif pd.to_numeric(price) < pd.to_numeric(value['Low']):

                    excel_df.loc[excel_df['Id'] == value['Id'], 'Low'] = price

                if value['sold'] == False and (zscore >= 2.2 or rate >= 5.0):

                    kilne_status[symbol] = False
                    excel_df.loc[excel_df['Id'] ==
                                 value['Id'], 's-date'] = time
                    excel_df.loc[excel_df['Id'] == value['Id'], 'sold'] = True
                    excel_df.loc[excel_df['Id'] == value['Id'], 'sell'] = price

            t_data = None

            try:
                excel_df.to_csv(f'results/data-1@loop.csv', header=True)
            except Exception as e:

                print(e)
        # sell(msg['data']['s'], pandas.to_numeric(msg['data']['c']),
        #      pandas.to_datetime(msg['data']['E'], unit='ms'))

        if '@aggTrade' in msg['stream']:

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

            if minute in points[symbol][day][hour].keys():

                if type == True:
                    points[symbol][day][hour][minute]['SC'] += 1
                    points[symbol][day][hour][minute]['SD'] += value
                    points[symbol][day][hour][minute]['SA'] = points[symbol][day][hour][minute]['SD'] / \
                        points[symbol][day][hour][minute]['SC']
                else:
                    points[symbol][day][hour][minute]['BC'] += 1
                    points[symbol][day][hour][minute]['BD'] += value
                    points[symbol][day][hour][minute]['BA'] = points[symbol][day][hour][minute]['BD'] / \
                        points[symbol][day][hour][minute]['BC']

                points[symbol][day][hour][minute]['V'] += value
                points[symbol][day][hour][minute]['DIFF'] = points[symbol][day][hour][minute]['BD'] - \
                    points[symbol][day][hour][minute]['SD']

            else:

                printTop(time, msg['T'])

                points[symbol][day][hour][minute] = {
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

            # buy(symbol=symbol, time=time, price=price,
            #     diff=points[symbol][day][hour-1]['DIFF'])

            points[symbol][day][hour] = {

            }
    else:

        points[symbol][day] = {

        }


def printTop(time, _t):

    global new_time
    difference = (time - new_time)
    total_seconds = difference.total_seconds()

    hours = divmod(total_seconds, 60)[0]

    if float(hours) >= 1:

        new_time = time
        global points

        if time.minute != 0 and time.minute != 1:

            listP = []
            for key, value in points.items():

                try:
                    t1 = list(value[time.day][time.hour].values())

                    if len(t1) > 2:

                        if t1[-1]['BC'] > (t1[-2]['BC'] + t1[-2]['SC']) and t1[-1]['BC'] > t1[-1]['SC']:

                            tps = (t1[-1]['SC'] + t1[-1]['BC'])/60

                            price = kilne_tracker[key].iloc[-1,
                                                            kilne_tracker[key].columns.get_loc('Close')]
                            zscore = kilne_tracker[key].iloc[-1,
                                                             kilne_tracker[key].columns.get_loc('48-zscore')]
                            if tps > 1 and zscore <= -1.5 and kilne_status[key] == False:

                                kilne_status[key] = True
                                excel_dff = {
                                    'Id': uuid.uuid1(),
                                    'Symbol': str(key),
                                    'Date': str(time),
                                    'Price': str(price),
                                    'BC': str(t1[-1]['BC']),
                                    'SC': str(t1[-1]['SC']),
                                    'TPS': str(tps),
                                    'Volume': str(t1[-1]['V']),
                                    'DIFF': str(t1[-1]['DIFF']),
                                    'ZSCORE': zscore,
                                    'High': str(price),
                                    'Low': str(price),
                                    'Close': str(price),
                                    'sell': 0,
                                    'sold': False
                                }
                                global excel_df
                                excel_df = excel_df.append(
                                    excel_dff, ignore_index=True)
                                try:
                                    excel_df.to_csv(
                                        f'results/data-1@loop.csv', header=True)
                                except Exception as e:

                                    print(e)
                                msg = (key + '\n' +
                                       'DIFF : ' +
                                       str(numpy.round(
                                           int(t1[-1]['DIFF']), 2)) + '\n'
                                       'V : ' +
                                       str(numpy.round(
                                           int(t1[-1]['V']), 2))+'\n'
                                       + 'SC : ' +
                                       str(numpy.round(
                                           int(t1[-1]['SC']), 2))+'\n'
                                       + ' BC : ' +
                                       str(numpy.round(
                                           int(t1[-1]['BC']), 2))+'\n'
                                       + 'TPS : ' +
                                       str(tps) + '\n')

                                send_message(str(msg), '-639026936')
                    else:
                        print(key + ' - ' + str(len(t1)))
                except Exception as e:

                    print(e)
                    print(key)


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
            self.multiplex_list.append(pairing.lower() + '@kline_1m')

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


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(getData, exchange_pairs)
    executor.map(readHistory, exchange_pairs)


t2 = time.perf_counter()


stream = Stream()
stream.start()
stream.bm.join()

# agg_trades = client.aggregate_trade_iter(symbol='WINGUSDT', start_str='1 hours ago UTC')

# data = pandas.DataFrame(agg_trades)
# data.columns = ['a', 'p', 'q', 'f', 'l', 'T', 'm', 'M']
# data['T'] = pandas.to_datetime(data['T'], unit='ms')

# print(data)
# data.to_csv(f'results/dataset_wing.csv', header=True)


# listP.append(
#     {
#         'key': key,
#         'value': t1['DIFF'],
#         'V': t1['V'],
#         'SC': t1['SC'],
#         'BC': t1['BC'],
#     }
# )
# newlist = sorted(listP, key=lambda x: x['value'], reverse=True)
# msg = '\t\tTOP 10 COINS\n\n'
# for i in newlist[0:10]:
