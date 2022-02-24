from os import close
import threading
import time
from binance.streams import ThreadedWebsocketManager
from models.node import Node
from numpy import e, sqrt
import numpy as np
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import AsyncClient, Client
from client import send_message
import pandas as pd
import matplotlib.pyplot as plt
import uuid

ordersList = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'drop_count', 'total', 'closed', 'buy_zscore', 'sell_zscore'])


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, dropRate, buyZscore):

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
        self.endDate = None
        self.drop_count = 1
        self.isSold = False
        self.sellZscore = None
        self.buyZscore = buyZscore


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


def setDatafFame(symbol):

    close = pd.to_numeric(kilne_tracker[symbol]['Close'])

    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    kilne_tracker[symbol]['48-zscore'] = zScore(
        window=48, close=close, volume=volume)
    kilne_tracker[symbol]['200-zscore'] = zScore(
        window=200, close=close, volume=volume)
    kilne_tracker[symbol]['484-zscore'] = zScore(
        window=484, close=close, volume=volume)

    kilne_tracker[symbol]['Close'] = pd.to_numeric(
        kilne_tracker[symbol]['Close'])

    # kilne_tracker[symbol].to_csv(
    #     f'zscore/'+symbol+'@data.csv', index=False, header=True)


def readHistory():
    print('start reading history of ' +
          str(len(exchange_pairs)) + ' USDT pairs...')

    for i in exchange_pairs:

        try:

            klines = client.get_historical_klines(
                symbol=i, interval=Client.KLINE_INTERVAL_15MINUTE, start_str="30 hours ago")

            data = pd.DataFrame(klines)

            data[0] = pd.to_datetime(data[0], unit='ms')

            data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                            'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

            data = data.drop(columns=['IGNORE',
                                      'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

            # data = data.set_index('Date')

            data['Close'] = pd.to_numeric(
                data['Close'], errors='coerce')

            kilne_tracker[i] = data

            print(i + ' is loaded...')

        except:

            print(i + ' caused error')

    print('Done.')
    print('Start Streaming.....')


def checkDrop(rate, order, price, time):

    if rate <= -7 and order.drop_count == 3:

        order.isSold = True
        ordersList[order.symbol]['isBuy'] = True

        message = '--- 48 zscore ---\n' + 'Order: run away\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + str(time) + '\nSupport price: ' + str(price) + '\ngrowth/drop: ' + str(rate) + '\nZscore: ' + \
            str(kilne_tracker[order.symbol].iloc[-1]['48-zscore'])

        send_message(message)

    elif rate <= -3 and order.drop_count < 3:

        order.drop_count += 1
        order.total += float(price)
        order.buyPrice = (order.total) / order.drop_count
        # order.sellPrice = order.buyPrice + (order.buyPrice * 0.05)

        message = '--- 48 zscore ---\n' + 'Order: support ' + str(order.drop_count - 1) + '\nSymbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nFrom: ' + \
            str(order.startDate) + '\nTo: ' + str(time) + '\nSupport price: ' + str(price) + '\ngrowth/drop: ' + str(rate) + '\nZscore: ' + \
            str(kilne_tracker[order.symbol].iloc[-1]['48-zscore'])

        send_message(message)

    else:
        order.sellPrice = price
        order.endDate = time
        order.rate = rate
        order.sellZscore = kilne_tracker[order.symbol].iloc[-1]['48-zscore']


def sell(s, time, price):

    list = ordersList[s]['list']
    p = float(price)

    try:
        for i in list:

            if i.isSold == False:

                symbol = i.symbol

                rate = ((float(price) - float(i.buyPrice)) /
                        float(i.buyPrice)) * 100

                # if kilne_tracker[symbol].iloc[-1]['48-zscore'] >= 2.6 and i.buyPrice <= p:
                if rate >= 2.0:

                    i.isSold = True
                    ordersList[symbol]['isBuy'] = True

                    i.sellPrice = price
                    i.endDate = time
                    i.rate = rate
                    i.sellZscore = kilne_tracker[symbol].iloc[-1]['48-zscore']

                    excel_df.loc[excel_df['id'] == i.id, 'sell'] = i.sellPrice
                    excel_df.loc[excel_df['id'] == i.id, 'endDate'] = i.endDate
                    excel_df.loc[excel_df['id'] == i.id, 'closed'] = i.isSold
                    excel_df.loc[excel_df['id'] == i.id,
                                 'drop_count'] = i.drop_count
                    excel_df.loc[excel_df['id'] == i.id,
                                 'sell_zscore'] = i.sellZscore
                    excel_df.loc[excel_df['id'] ==
                                 i.id, 'growth/drop'] = i.rate

                    excel_df.to_csv(f'results/data@zscore-15m.csv')

                    message = '--- 48 zscore ---\n' + 'Id: ' + str(i.id) + '\nOrder: Sell\n' + 'Symbol: ' + \
                        str(i.symbol) + '\nInterval: ' + str(i.interval)+'\nBuy price: ' + \
                        str(i.buyPrice) + '\nSell price: '+str(kilne_tracker[symbol].iloc[-1]['Close'])+'\nFrom: ' + \
                        str(i.startDate) + '\nTo: ' + str(time) + '\ngrowth/drop: ' + str(rate) + '\nDrop count: ' + str(i.drop_count - 1) + '\nSell zscore: ' + \
                        str(kilne_tracker[symbol].iloc[-1]['48-zscore']
                            ) + '\nBuy zscore: ' + str(i.buyZscore)

                    send_message(message)

                else:

                    checkDrop(rate, i, p, time)

                    excel_df.loc[excel_df['id'] == i.id, 'sell'] = i.sellPrice
                    excel_df.loc[excel_df['id'] == i.id, 'endDate'] = i.endDate
                    excel_df.loc[excel_df['id'] == i.id, 'closed'] = i.isSold
                    excel_df.loc[excel_df['id'] == i.id,
                                 'drop_count'] = i.drop_count
                    excel_df.loc[excel_df['id'] == i.id,
                                 'sell_zscore'] = i.sellZscore
                    excel_df.loc[excel_df['id'] ==
                                 i.id, 'growth/drop'] = i.rate

                    excel_df.to_csv(f'results/data@zscore-15m.csv')

    except Exception as e:
        print(e)


def buy(symbol, time):

    zscore = kilne_tracker[symbol].iloc[-1,
                                        kilne_tracker[symbol].columns.get_loc('48-zscore')]
    zscore_484 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('484-zscore')]
    zscore_200 = kilne_tracker[symbol].iloc[-1,
                                            kilne_tracker[symbol].columns.get_loc('200-zscore')]

    if (zscore <= -2.5 and ordersList[symbol]['isBuy'] == False and zscore_484 > -1 and zscore_200 > -1) or zscore <= -4.0:

        ordersList[symbol]['isBuy'] = True
        ordersList
        order = Order(
            id=uuid.uuid1(),
            type='zscore',
            symbol=symbol,
            interval='15m',
            buyPrice=kilne_tracker[symbol].iloc[-1]['Close'],
            sellPrice=kilne_tracker[symbol].iloc[-1]['Close'] +
            (kilne_tracker[symbol].iloc[-1]['Close'] * 0.05),
            amount=500,
            startDate=time,
            dropRate=5,
            buyZscore=zscore
        )
        ordersList[symbol]['list'].append(order)

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
            'drop_count': order.drop_count,
            'total': order.total,
            'closed': order.isSold,
            'growth/drop': order.rate,
            'buy_zscore': order.buyZscore,
            'sell_zscore': order.sellZscore
        }
        global excel_df
        excel_df = excel_df.append(msg, ignore_index=True)

        excel_df.to_csv(f'results/data@zscore-15m.csv', header=False)

        message = '--- 48 zscore  ---\n' + 'Id: ' + str(order.id) + '\nOrder: Buy\n' + 'Symbol: ' + \
            str(order.symbol) + '\nInterval: ' + str(order.interval)+'\nBuy price: ' + \
            str(order.buyPrice) + '\nFrom: ' + \
            str(order.startDate) + '\nZscore: ' + str(zscore)
        send_message(message)


def init():
    for pair in exchange_pairs:
        ordersList[pair] = {}
        ordersList[pair]['list'] = []
        ordersList[pair]['isBuy'] = False


def realtime(msg):

    if 'data' in msg:
        # Your code
        handle_socket(msg['data'])

    else:
        stream.stream_error = True


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

    setDatafFame(symbol=symbol)

    # except:

    #     print('Error while updating data...')


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = msg['k']['c']
    symbol = msg['s']

    updateFrame(symbol, msg)

    sell(s=symbol, time=time, price=close)


init()

readHistory()


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@kline_15m')
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
