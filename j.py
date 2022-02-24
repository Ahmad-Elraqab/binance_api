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

FILE_NAME = 'RSI-15M-10%'
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
c_df = pd.DataFrame(columns=['s', 'status', 'price', 'set-buy', 'buy'])


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, dropRate, volume, rsi):

        self.id = id
        self.type = type
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.sellPrice = sellPrice
        self.amount = amount
        self.startDate = startDate
        self.dropRate = dropRate
        self.total = buyPrice
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

    global c_df

    print('start reading history of ' + str(i) + ' USDT pairs...')

    klines = client.get_historical_klines(
        symbol=i, interval=H_HISTORY, start_str="3 days ago")

    data = pd.DataFrame(klines)

    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    data['rsi_14'] = get_rsi(data['Close'], 14)
    c_df = c_df.append(
        {'s': i, 'status': False, 'price': data.iloc[-1, 4], 'buy': False, 'set-buy': False}, ignore_index=True)

    kilne_tracker[i] = data

    print(i + ' is loaded...')


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


def calcRSI(symbol):
    kilne_tracker[symbol]['Close'] = pd.to_numeric(
        kilne_tracker[symbol]['Close'])

    kilne_tracker[symbol]['rsi_14'] = get_rsi(
        kilne_tracker[symbol]['Close'], 14)


def calcDailyChange(symbol):

    index = len(kilne_tracker[symbol])
    kilne_tracker[symbol].loc[index-1, 'd-ch'] = (kilne_tracker[symbol].loc[index-1, 'Close'] -
                                                  kilne_tracker[symbol].loc[index - 96, 'Close']) / kilne_tracker[symbol].loc[index-1, 'Close'] * 100


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


def checkCross(symbol, msg):

    try:
        vwap20 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_20')]
        vwap48 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_48')]
        vwap84 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('DIF_84')]

        check = c_df.loc[c_df['s'] == symbol]

        # if vwap20 >= vwap48 and vwap48 >= vwap84 and check.iloc[0]['status'] == False:
        if vwap20 >= vwap48 and vwap20 >= vwap84 and check.iloc[0]['status'] == False:

            c_df.loc[c_df['s'] == symbol,
                     'price'] = pd.to_numeric(msg['k']['c'])
            c_df.loc[c_df['s'] == symbol, 'status'] = True

        elif (vwap20 <= vwap48 or vwap48 <= vwap84) and check.iloc[0]['status'] == True:

            c_df.loc[c_df['s'] == symbol, 'status'] = False
            # c_df.loc[c_df['s'] == symbol, 'set-buy'] = False

        # elif pd.to_numeric(msg['k']['c']) <= vwap20 and check.iloc[0]['status'] == True and check.iloc[0]['set-buy'] == False:

        #     c_df.loc[c_df['s'] == symbol, 'set-buy'] = True

    except:
        # print('.')
        pass


def buy(symbol, time):

    try:
        check = c_df.loc[c_df['s'] == symbol]

        if check.iloc[0]['status'] == True and check.iloc[0]['buy'] == False:

            c_df.loc[c_df['s'] == symbol, 'buy'] = True

            order = Order(
                id=uuid.uuid1(),
                type='rsi',
                symbol=symbol,
                interval=INTERVAL,
                buyPrice=kilne_tracker[symbol].loc[len(
                    kilne_tracker[symbol])-1, 'Close'],
                sellPrice=kilne_tracker[symbol].loc[len(kilne_tracker[symbol])-1, 'Close'] +
                (kilne_tracker[symbol].loc[len(
                    kilne_tracker[symbol])-1, 'Close'] * 0.05),
                amount=500,
                startDate=time,
                dropRate=5,
                volume=kilne_tracker[symbol].loc[len(
                    kilne_tracker[symbol])-1, 'Volume'],
                rsi=kilne_tracker[symbol].loc[len(
                    kilne_tracker[symbol])-1, 'rsi_14'],
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
    except Exception as e:
        # print('error buy ' + str(symbol))
        pass


def checkSell(rate, order, price, time):

    # vwap20 = kilne_tracker[order.symbol].iloc[-1,
    #                                           kilne_tracker[order.symbol].columns.get_loc('DIF_20')]
    # vwap48 = kilne_tracker[order.symbol].iloc[-1,
    #                                           kilne_tracker[order.symbol].columns.get_loc('DIF_48')]
    vwap84 = kilne_tracker[order.symbol].iloc[-1,
                                              kilne_tracker[order.symbol].columns.get_loc('DIF_84')]

    order.sellPrice = price
    order.endDate = time
    order.rate = rate
    order.sellZscore = 0

    if price > order.high:
        order.high = price

    if price < order.low:
        order.low = price

    # difference = (order.endDate - order.startDate)
    # total_seconds = difference.total_seconds()
    # hours = divmod(total_seconds, 60)[0]
    drop = (price - vwap84) / price * 100
    print(drop)
    
    if rate >= 10.0:
        # if vwap20 < vwap48:

        c_df.loc[c_df['s'] == order.symbol, 'status'] = False
        # c_df.loc[c_df['s'] == order.symbol, 'set-buy'] = False
        c_df.loc[c_df['s'] == order.symbol, 'buy'] = False

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
    excel_df.loc[excel_df['id'] == order.id, 'drop_count'] = order.drop_count
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
        pass


def updateFrame(symbol, msg):

    # print(msg)
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
    calcRSI(symbol=symbol)
    calcDailyChange(symbol=symbol)

    checkCross(symbol, msg)
    buy(symbol, time)
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
# time.sleep(5)

stream.start()
stream.bm.join()
