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

FILE_NAME = 'RSI-15M-2%'
coin_list = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'closed', 'high', 'low', 'Volume', 'RSI'])
ordersList = {}

INTERVAL = '15m'
H_HISTORY = Client.KLINE_INTERVAL_15MINUTE
PART = '-'
temp = False
tempTime = None


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, volume, rsi):

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

        coin_list[i] = {'s': i, 'status': False,
                        'price': data.iloc[-1, 4], 'buy': False, 'set-buy': False}

        kilne_tracker[i] = data

        # data.to_csv(f'stream/' + i+'.csv')
        # print(i + ' is loaded...')
    except Exception as e:
        pass


def get_rsi(close, lookback):
    ret = close.diff()
    up = []
    down = []
    for i in range(len(ret)):
        if ret[i] < 0:
            up.append(0)
            down.append(ret[i])
        else:
            up.append(ret[i])
            down.append(0)

    up_series = pd.Series(up)
    down_series = pd.Series(down).abs()

    up_ewm = up_series.ewm(com=lookback - 1, adjust=False).mean()
    down_ewm = down_series.ewm(com=lookback - 1, adjust=False).mean()

    rs = up_ewm/down_ewm

    rsi = 100 - (100 / (1 + rs))

    rsi_df = pd.DataFrame(rsi).rename(
        columns={0: 'rsi'}).set_index(close.index)

    rsi_df = rsi_df.dropna()

    return rsi_df[3:]


def calcVWAP(symbol, msg, inte):

    high = pd.to_numeric(kilne_tracker[symbol]['High'])
    low = pd.to_numeric(kilne_tracker[symbol]['Low'])
    close = pd.to_numeric(kilne_tracker[symbol]['Close'])
    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    # df = kilne_tracker[symbol].iloc[1:50]

    # df = kilne_tracker[symbol].iloc[-1:-14,
    #                                 kilne_tracker[symbol].columns.get_loc('Close')].sum()

    # print(df)

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
    set_buy = coin_list[symbol]['set-buy']
    length = len(kilne_tracker[symbol])-1

    if pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) <= pd.to_numeric(vwap20) and status == False and vwap20 > vwap48 and vwap48 > vwap84:

        coin_list[symbol]['status'] = True

    elif pd.to_numeric(vwap20) <= pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) and set_buy == False and status == True:

        coin_list[symbol]['set-buy'] = True


def buy(symbol, time):

    try:
        status = coin_list[symbol]['status']
        set_buy = coin_list[symbol]['set-buy']
        buy_ = coin_list[symbol]['buy']
        length = len(kilne_tracker[symbol])-1

        list = [x for x in ordersList['list'] if x.isSold == False]

        if status == True and buy_ == False and set_buy == True and len(list) < 20 and pd.to_numeric(kilne_tracker[symbol].loc[length, 'Close']) >= pd.to_numeric(kilne_tracker[symbol].loc[length, 'DIF_20']):

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
                rsi=kilne_tracker[symbol].loc[length, 'rsi_14'],
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
                'high': order.high,
                'low': order.low,
                'Volume': order.volume,
                'RSI': order.rsi,
            }
            global excel_df
            excel_df = excel_df.append(msg, ignore_index=True)

            excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')

            message = '--- 10% ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
                str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
                str(order.buyPrice) + '\nFrom: ' + \
                str(order.startDate)

            send_message(message)

        else:

            coin_list[symbol]['status']
            coin_list[symbol]['set-buy']

    except Exception as e:
        # print('error buy ' + str(symbol))
        print('.')
        pass


def checkSell(rate, order, price, time):

    # vwap20 = kilne_tracker[order.symbol].iloc[-1,
    #                                           kilne_tracker[order.symbol].columns.get_loc('DIF_20')]

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

    if rate > 2.0 or rate <= -1.5 or hours >= 90.0:

        coin_list[order.symbol]['status']
        coin_list[order.symbol]['buy']

        order.isSold = True
        ordersList[order.symbol]['date'] = datetime.now()

        message = '--- 10% ---\n' + 'Order: Sell\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + \
            str(time) + '\ngrowth/drop: ' + str(rate)

        send_message(message)

    excel_df.loc[excel_df['id'] == order.id, 'sell'] = order.sellPrice
    excel_df.loc[excel_df['id'] == order.id, 'endDate'] = order.endDate
    excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold
    excel_df.loc[excel_df['id'] == order.id, 'buy'] = order.buyPrice
    excel_df.loc[excel_df['id'] == order.id, 'sell_zscore'] = order.sellZscore
    excel_df.loc[excel_df['id'] == order.id, 'growth/drop'] = order.rate
    excel_df.loc[excel_df['id'] == order.id, 'high'] = order.high
    excel_df.loc[excel_df['id'] == order.id, 'low'] = order.low
    excel_df.loc[excel_df['id'] == order.id, 'Volume'] = order.volume
    excel_df.loc[excel_df['id'] == order.id, 'RSI'] = order.rsi

    excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')


def sell(s, time, price):

    try:
        list = ordersList['list']
        p = float(price)

        for i in list:

            if i.isSold == False and i.symbol == s:

                rate = ((float(price) - float(i.buyPrice)) /
                        float(i.buyPrice)) * 100

                checkSell(rate, i, p, time)
    except:
        # print('error sell ' + str(s))
        # print('.')
        pass


def updateFrame(symbol, msg):

    try:
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

        
        kilne_tracker[symbol]['rsi_14'] = get_rsi(kilne_tracker[symbol]['Close'], 14)
            
        calcVWAP(symbol=symbol, msg=msg, inte=20)
        calcVWAP(symbol=symbol, msg=msg, inte=48)
        calcVWAP(symbol=symbol, msg=msg, inte=84)

        checkTouch(symbol=symbol)
        sell(symbol, time, msg['k']['c'])
    except:
        pass


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
