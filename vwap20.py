from ast import And
from datetime import datetime
import threading
import uuid
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from pandas.core.frame import DataFrame
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import pandas as pd
import time
import os
import errno
import concurrent.futures

FILE_NAME = 'RSI-15M-stream-1'
coin_list = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'closed', 'status', '15', '30', '45', '60', 'vwap20'])
ordersList = {}

INTERVAL = '15m'
H_HISTORY = Client.KLINE_INTERVAL_15MINUTE
PART = '-'
temp = False
tempTime = None


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, volume, rsi, status):

        self.id = id
        self.type = type
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.sellPrice = sellPrice
        self.amount = amount
        self.startDate = startDate
        self.rate = None
        self.endDate = startDate
        self.isSold = False
        self.isHold = False
        self.hold = buyPrice
        self.high = buyPrice
        self.low = buyPrice
        self.volume = volume
        self.rsi = rsi
        self.status = status


def readHistory(i):

    try:
        global c_df

        # print('start reading history of ' + str(i) + ' USDT pairs...')

        klines = client.get_historical_klines(
            symbol=i, interval=H_HISTORY, start_str="1 days ago")

        data = pd.DataFrame(klines)

        data[0] = pd.to_datetime(data[0], unit='ms')

        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                        'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

        data = data.drop(columns=['IGNORE',
                                  'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

        data['Close'] = pd.to_numeric(
            data['Close'], errors='coerce')

        # c_df = c_df.append(
        #     {'s': i, 'status': False, 'price': data.iloc[-1, 4], 'buy': False, 'set-buy': False}, ignore_index=True)

        coin_list[i] = {'s': i, 'status': None,
                        'price': data.iloc[-1, 4], 'buy': False, 'set-buy': None}

        kilne_tracker[i] = data

        # data.to_csv(f'stream/' + i+'.csv')
        # print(i + ' is loaded...')
    except Exception as e:
        pass


def calcVWAP(symbol, msg, inte):

    high = pd.to_numeric(kilne_tracker[symbol]['High'])
    low = pd.to_numeric(kilne_tracker[symbol]['Low'])
    close = pd.to_numeric(kilne_tracker[symbol]['Close'])
    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    value1 = ((high + low + close) / 3 * volume).rolling(inte).sum()

    value2 = volume.rolling(inte).sum()

    kilne_tracker[symbol]['DIF_' + str(inte)] = value1 / value2


def checkTouch(symbol):

    vwap20 = pd.to_numeric(
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('DIF_20')])
    vwap48 = pd.to_numeric(
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('DIF_48')])
    vwap84 = pd.to_numeric(
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('DIF_84')])

    status = coin_list[symbol]['status']
    length = len(kilne_tracker[symbol])-1

    # if pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) <= pd.to_numeric(vwap20) and status == False and vwap20 > vwap48 and vwap48 > vwap84:

    #     coin_list[symbol]['status'] = True

    if pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) >= pd.to_numeric(vwap20) and pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) >= pd.to_numeric(vwap48) and pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) >= pd.to_numeric(vwap84):

        coin_list[symbol]['status'] = True


def buy(symbol, time):

    try:
        status = coin_list[symbol]['status']
        buy_ = coin_list[symbol]['buy']
        length = len(kilne_tracker[symbol])-1

        vwap20 = pd.to_numeric(
            kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('DIF_20')])

        close = kilne_tracker[symbol].iloc[-1,
                                           kilne_tracker[symbol].columns.get_loc('Close')]
        open = kilne_tracker[symbol].iloc[-1,
                                          kilne_tracker[symbol].columns.get_loc('Open')]

        low = kilne_tracker[symbol].iloc[-1,
                                         kilne_tracker[symbol].columns.get_loc('Low')]

        coin_6h = (kilne_tracker['BTCUSDT'].loc[length, 'Close'] - kilne_tracker['BTCUSDT'].loc[length -
                   8, 'Close']) / kilne_tracker['BTCUSDT'].loc[length, 'Close'] * 100
        vwap = (close - vwap20) / close * 100

        btc_price = (kilne_tracker['BTCUSDT'].loc[length, 'Close'] - kilne_tracker['BTCUSDT'].loc[length -
                     1, 'Close']) / kilne_tracker['BTCUSDT'].loc[length, 'Close'] * 100

        check = pd.to_numeric(close) > pd.to_numeric(open)

        rate = None

        if check == True:
            rate = (open - low) > np.abs(close - open)
        else:
            rate = (close - low) > np.abs(close - open)

        global excel_df

        if buy_ == False and rate == True and status == True:

            print(btc_price)
            print(coin_list[symbol]['set-buy'])

            if btc_price < 0.12 and coin_list[symbol]['set-buy'] == None:
                coin_list[symbol]['set-buy'] = True

                coin_list[symbol]['buy'] = True

                order = Order(
                    id=uuid.uuid1(),
                    type='rsi',
                    symbol=symbol,
                    interval=INTERVAL,
                    buyPrice=kilne_tracker[symbol].loc[length, 'Close'],
                    sellPrice=kilne_tracker[symbol].loc[length, 'Close'] +
                    (kilne_tracker[symbol].loc[length, 'Close'] * 0.05),
                    amount=500,
                    startDate=time,
                    volume=kilne_tracker[symbol].loc[length, 'Volume'],
                    rsi=0,
                    status=status
                )
                ordersList['list'].append(order)

                msg = {
                    'id': order.id,
                    'symbol': order.symbol,
                    'type': order.type,
                    'interval': order.interval,
                    'amount': order.amount,
                    'startDate': order.startDate,
                    'endDate': order.endDate,
                    'buy': order.buyPrice,
                    'sell': order.sellPrice,
                    'closed': order.isSold,
                    'growth/drop': order.rate,
                    'status': status,
                    'vwap20': vwap,
                    'high': order.high,
                    'low': order.low,
                    'Volume': order.volume,
                    'RSI': order.rsi,
                    'BTC': btc_price,
                    'coin': (kilne_tracker[symbol].loc[length, 'Close'] - kilne_tracker[symbol].loc[length-1, 'Close']) / kilne_tracker[symbol].loc[length, 'Close'] * 100,
                    'coin-6h': coin_6h,
                    'V-BTC': kilne_tracker[symbol].loc[length, 'Volume'],
                    'V-B': (pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length, 'Volume']) - pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length-1, 'Volume'])) / pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length, 'Volume']) * 100,
                    'V-C': (pd.to_numeric(kilne_tracker[symbol].loc[length, 'Volume']) - pd.to_numeric(kilne_tracker[symbol].loc[length-1, 'Volume'])) / pd.to_numeric(kilne_tracker[symbol].loc[length, 'Volume']) * 100,


                }
                excel_df = excel_df.append(msg, ignore_index=True)

                excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')

                message = '--- 10% ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
                    str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
                    str(order.buyPrice) + '\nFrom: ' + \
                    str(order.startDate) + '\n DIFF: ' + str(rate)

            elif btc_price > 0.12 and coin_list[symbol]['set-buy'] == None:
                coin_list[symbol]['set-buy'] = False

            elif btc_price < -0.12 and coin_list[symbol]['set-buy'] == False:
                coin_list[symbol]['set-buy'] = True

                coin_list[symbol]['buy'] = True

                order = Order(
                    id=uuid.uuid1(),
                    type='rsi',
                    symbol=symbol,
                    interval=INTERVAL,
                    buyPrice=kilne_tracker[symbol].loc[length, 'Close'],
                    sellPrice=kilne_tracker[symbol].loc[length, 'Close'] +
                    (kilne_tracker[symbol].loc[length, 'Close'] * 0.05),
                    amount=500,
                    startDate=time,
                    volume=kilne_tracker[symbol].loc[length, 'Volume'],
                    rsi=0,
                    status=status
                )
                ordersList['list'].append(order)

                msg = {
                    'id': order.id,
                    'symbol': order.symbol,
                    'type': order.type,
                    'interval': order.interval,
                    'amount': order.amount,
                    'startDate': order.startDate,
                    'endDate': order.endDate,
                    'buy': order.buyPrice,
                    'sell': order.sellPrice,
                    'closed': order.isSold,
                    'growth/drop': order.rate,
                    'status': status,
                    'vwap20': vwap,
                    'high': order.high,
                    'low': order.low,
                    'Volume': order.volume,
                    'RSI': order.rsi,
                    'BTC': btc_price,
                    'coin': (kilne_tracker[symbol].loc[length, 'Close'] - kilne_tracker[symbol].loc[length-1, 'Close']) / kilne_tracker[symbol].loc[length, 'Close'] * 100,
                    'coin-6h': coin_6h,
                    'V-BTC': kilne_tracker[symbol].loc[length, 'Volume'],
                    'V-B': (pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length, 'Volume']) - pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length-1, 'Volume'])) / pd.to_numeric(kilne_tracker['BTCUSDT'].loc[length, 'Volume']) * 100,
                    'V-C': (pd.to_numeric(kilne_tracker[symbol].loc[length, 'Volume']) - pd.to_numeric(kilne_tracker[symbol].loc[length-1, 'Volume'])) / pd.to_numeric(kilne_tracker[symbol].loc[length, 'Volume']) * 100,


                }
                excel_df = excel_df.append(msg, ignore_index=True)

                excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')

                message = '--- 10% ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
                    str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
                    str(order.buyPrice) + '\nFrom: ' + \
                    str(order.startDate) + '\n DIFF: ' + str(rate)

            # send_message(message)
            else:
                coin_list[symbol]['set-buy'] == None
        else:

            coin_list[symbol]['status'] = False
            coin_list[symbol]['set-buy'] = None
            # coin_list[symbol]['set-buy'] = False

    except Exception as e:
        # print('error buy ' + str(symbol))
        # print('.')
        pass


def checkSell(rate, order, price, time):

    order.sellPrice = price
    order.endDate = time
    order.rate = rate
    order.sellZscore = 0

    if price > order.high:
        order.high = price

    elif price < order.low:
        order.low = price

    difference = (order.endDate - order.startDate)
    total_seconds = difference.total_seconds()
    hours = divmod(total_seconds, 60)[0]

    # if (order.status == True and rate > 1.5) or (order.status == False and rate > 0.5) or (rate <= -3.0) or hours >= 45.0:
    if rate > 0.5 or (rate <= -3.0) or hours >= 45.0:

        coin_list[order.symbol]['status'] = False
        coin_list[order.symbol]['buy'] = False
        coin_list[order.symbol]['set-buy'] = None

        order.isSold = True
        ordersList[order.symbol]['date'] = datetime.now()

        message = '--- 10% ---\n' + 'Order: Sell\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + \
            str(time) + '\ngrowth/drop: ' + str(rate)

        # send_message(message)

    excel_df.loc[excel_df['id'] == order.id, 'sell'] = order.sellPrice
    excel_df.loc[excel_df['id'] == order.id, 'endDate'] = order.endDate
    excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold
    excel_df.loc[excel_df['id'] == order.id, 'buy'] = order.buyPrice
    excel_df.loc[excel_df['id'] == order.id, 'sell_zscore'] = order.sellZscore
    excel_df.loc[excel_df['id'] == order.id, 'growth/drop'] = order.rate
    excel_df.loc[excel_df['id'] == order.id, 'high'] = order.high
    excel_df.loc[excel_df['id'] == order.id, 'low'] = order.low

    if hours == 15.0:
        excel_df.loc[excel_df['id'] == order.id, '15'] = rate
    elif hours == 30.0:
        excel_df.loc[excel_df['id'] == order.id, '30'] = rate
    elif hours == 45.0:
        excel_df.loc[excel_df['id'] == order.id, '45'] = rate
    elif hours == 60.0:
        excel_df.loc[excel_df['id'] == order.id, '60'] = rate

    excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')


def sell(s, time, price):

    list = ordersList['list']
    p = float(price)

    for i in list:

        if i.isSold == False and i.symbol == s:

            rate = ((float(price) - float(i.buyPrice)) /
                    float(i.buyPrice)) * 100

            checkSell(rate, i, p, time)


def updateFrame(symbol, msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    check = np.where(
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Date')] == time, True, False)

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

        # list = [x for x in ordersList['list'] if x.isSold == False]
        # if len(list) < 20:
        buy(symbol, time)

        kilne_tracker[symbol] = kilne_tracker[symbol].append({
            'Date': time,
            'Open': msg['k']['o'],
            'High': msg['k']['h'],
            'Low': msg['k']['l'],
            'Close': msg['k']['c'],
            'Volume': msg['k']['v'],
            'Quote_Volume': msg['k']['q'],
        }, ignore_index=True)

    calcVWAP(symbol=symbol, msg=msg, inte=20)
    calcVWAP(symbol=symbol, msg=msg, inte=48)
    calcVWAP(symbol=symbol, msg=msg, inte=84)

    checkTouch(symbol=symbol)
    sell(symbol, time, msg['k']['c'])


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = msg['k']['c']
    symbol = msg['s']

    updateFrame(symbol=symbol, msg=msg)


def realtime(msg):

    if 'data' in msg:
        handle_socket(msg['data'])

    else:
        stream.stream_error = True


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@kline_'+INTERVAL)
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


send_message('NEW')


def init():
    ordersList['list'] = []
    for pair in exchange_pairs:
        ordersList[pair] = {}


init()

t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(readHistory, exchange_pairs)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} seconds')

stream = Stream()
time.sleep(5)

stream.start()
stream.bm.join()
