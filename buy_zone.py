from os import close
from models.node import Node
from numpy import sqrt
import numpy as np
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import Client
from client import send_message
import pandas as pd
import matplotlib.pyplot as plt

ordersList = []
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
klines = client.get_historical_klines(
    symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_5MINUTE, start_str="1 day ago")

data = pd.DataFrame(klines)

data[0] = pd.to_datetime(data[0], unit='ms')


data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']
# data = data.set_index('Date')

data = data.drop(columns=['IGNORE',
                          'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

data['Close'] = pd.to_numeric(
    data['Close'], errors='coerce')


class Order:
    def __init__(self, type, symbol, interval, price, amount, startDate, volume, qVolume, buyPrice, buyBlack, buyRed, buyRatio, buyBlue):
        self.type = type
        self.price = price
        self.stopLose = False
        self.sellProfit = False
        self.buyPrice = buyPrice
        self.symbol = symbol
        self.interval = interval
        self.gainProfit = 0
        self.amount = amount
        self.startDate = startDate
        self.endDate = None
        self.volume = volume
        self.qVolume = qVolume
        self.sellList = []
        self.sellVolume = []
        self.buyBlack = buyBlack,
        self.buyRed = buyRed,
        self.buyBlue = buyBlue,
        self.buyRatio = buyRatio,
        self.sellBlack = 0,
        self.sellRed = 0,
        self.sellBlue = 0,
        self.sellRetio = 0,


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


def getData(symbol):

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="13 sep 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    close = pd.to_numeric(data['Close'])
    open = pd.to_numeric(data['Open'])
    high = pd.to_numeric(data['High'])
    low = pd.to_numeric(data['Low'])
    volume = pd.to_numeric(data['Volume'])

    data['48-zscore'] = zScore(window=48, close=close, volume=volume)
    data['199-zscore'] = zScore(window=199, close=close, volume=volume)
    data['484-zscore'] = zScore(window=484, close=close, volume=volume)

    a = data['High'].shift(1).rolling(14).max()
    b = data['High'].shift(1).rolling(15).max()
    c = data['Low'].shift(1).rolling(14).min()
    d = data['Low'].shift(1).rolling(15).min()

    l = data['Close'].shift(15)
    max = DataFrame(columns=['max'], data=np.where(a < b, l, a))
    min = DataFrame(columns=['min'], data=np.where(c > d, l, c))

    data['Max'] = max
    data['Min'] = min
    data['volatility ratio'] = (high - low) / (data['Max'] - data['Min'])

    print(data['volatility ratio'])

    df = pd.DataFrame(columns=['symbol',
                               'type',
                               'interval',
                               'buyPrice',
                               'sellPrice',
                               'buyAmount',
                               'gainProfit',
                               'gainAmount',
                               'totalAmount',
                               'startDate',
                               'endDate',
                               'avgDate',
                               'sellVolume',
                               'volume',
                               'quoteVolume',
                               'sellList',
                               'buyBlack',
                               'buyRed',
                               'buyBlue',
                               'buyRatio',
                               'sellBlack',
                               'sellRed',
                               'sellBlue',
                               'sellRetio',
                               ])

    for index, row in data.iterrows():

        if row['48-zscore'] <= -2.0 and data['volatility ratio'] >= 0.4:
            ordersList.append(
                Order(symbol=symbol, type='48', interval='30M', buyPrice=row['Close'], price=[row['Close']], amount=500,
                      startDate=row['Date'], volume=row['Volume'], qVolume=row['Quote_Volume'], buyBlack=pd.to_numeric(
                          row['48-zscore']),
                      buyRed=pd.to_numeric(row['484-zscore']),  buyBlue=pd.to_numeric(row['199-zscore']), buyRatio=pd.to_numeric(row['volatility ratio'])))

        else:
            for order in ordersList:

                rate = ((row['Close'] - order.price[-1]) /
                        order.price[-1]) * 100

                if order.type == '48':

                    # if row['48-zscore'] >= 2.0 and rate > 0.0:
                    if rate >= 2.5:
                        order.gainProfit += rate
                        order.endDate = row['Date']
                        order.price.append(row['Close'])
                        order.sellList.append(row['Date'])
                        order.sellBlack = pd.to_numeric(row['48-zscore'])
                        order.sellRed = pd.to_numeric(row['484-zscore'])
                        order.sellBlue = pd.to_numeric(row['199-zscore'])
                        order.sellRatio = pd.to_numeric(
                            row['volatility ratio'])

                        new_row = {'symbol': order.symbol,
                                   'type': order.type,
                                   'interval': order.interval,
                                   'buyPrice': order.buyPrice,
                                   'sellPrice': order.price,
                                   'buyAmount': order.amount,
                                   'gainProfit': order.gainProfit,
                                   'gainAmount': order.gainProfit / 100 * order.amount,
                                   'totalAmount': (order.gainProfit / 100 + 1) * order.amount,
                                   'startDate': order.startDate,
                                   'endDate': order.endDate,
                                   'sellVolume': order.sellVolume,
                                   'volume': order.volume,
                                   'quoteVolume': order.qVolume,
                                   'sellList': order.sellList,
                                   'buyBlack': order.buyBlack,
                                   'buyRed': order.buyRed,
                                   'buyBlue': order.buyBlue,
                                   'buyRatio': order.buyRatio,
                                   'sellBlack': order.sellBlack,
                                   'sellRed': order.sellRed,
                                   'sellBlue': order.sellBlue,
                                   'sellRatio': order.sellRatio
                                   }
                        ordersList.remove(order)
                        df = df.append(
                            new_row, ignore_index=True)

                    # elif rate <= 10.0:
                    #     order.gainProfit += rate
                    #     order.endDate = row['Date']
                    #     order.sellVolume.append(row['Volume'])
                    #     order.price.append(row['Close'])
                    #     order.sellList = row['Date']
                    #     order.sellBlack = row['48-zscore']
                    #     order.sellRed = row['484-zscore']
                    #     order.sellBlue = row['199-zscore']
                    #     order.sellRatio = row['volatility ratio']

                    #     new_row = {'symbol': order.symbol,
                    #                'type': order.type,
                    #                'interval': order.interval,
                    #                'buyPrice': order.buyPrice,
                    #                'sellPrice': order.price,
                    #                'buyAmount': order.amount,
                    #                'gainProfit': order.gainProfit,
                    #                'gainAmount': order.gainProfit / 100 * order.amount,
                    #                'totalAmount': (order.gainProfit / 100 + 1) * order.amount,
                    #                'startDate': order.startDate,
                    #                'endDate': order.endDate,
                    #                'sellVolume': order.sellVolume,
                    #                'volume': order.volume,
                    #                'quoteVolume': order.qVolume,
                    #                'sellList': order.sellList,
                    #                'buyBlack': order.buyBlack,
                    #                'buyRed': order.buyRed,
                    #                'buyBlue': order.buyBlue,
                    #                'buyRatio': order.buyRatio,
                    #                'sellBlack': order.sellBlack,
                    #                'sellRed': order.sellRed,
                    #                'sellBlue': order.sellBlue,
                    #                'sellRatio': order.sellRatio
                    #                }
                    #     ordersList.remove(order)
                    #     df = df.append(
                    #         new_row, ignore_index=True)

    print(len(ordersList))
    df.to_csv(f'files/data2.csv', index=False,
              header=True, mode='a')
    ordersList.clear()
    # data.to_csv(f'files/data.csv', index=False, header=True)


def setDatafFame():

    close = pd.to_numeric(data['Close'])
    open = pd.to_numeric(data['Open'])
    high = pd.to_numeric(data['High'])
    low = pd.to_numeric(data['Low'])
    volume = pd.to_numeric(data['Volume'])

    data['48-zscore'] = zScore(window=48, close=close, volume=volume)
    data['199-zscore'] = zScore(window=199, close=close, volume=volume)
    data['484-zscore'] = zScore(window=484, close=close, volume=volume)

    a = data['High'].shift(1).rolling(14).max()
    b = data['High'].shift(1).rolling(15).max()
    c = data['Low'].shift(1).rolling(14).min()
    d = data['Low'].shift(1).rolling(15).min()

    l = data['Close'].shift(15)
    max = DataFrame(columns=['max'], data=np.where(a < b, l, a))
    min = DataFrame(columns=['min'], data=np.where(c > d, l, c))

    data['Max'] = max
    data['Min'] = min
    data['volatility ratio'] = (high - low) / (data['Max'] - data['Min'])

    print(data)


def handle_socket(msg):

    volume = float(msg['k']['v'])
    qVolume = float(msg['k']['q'])
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = float(msg['k']['c'])
    open = float(msg['k']['o'])
    high = float(msg['k']['h'])
    low = float(msg['k']['l'])

    rate = float(msg['k']['c']) / float(msg['k']['o'])
    symbol = msg['s']
    global data
    check = np.where(data.iloc[-1][0] == time, True, False)

    if check == True:

        print(check)
        data['Open'] = data['Open'].replace(
            data.iloc[-1]['Open'], float(msg['k']['o']))

        data['Close'] = data['Close'].replace(
            data.iloc[-1]['Close'], float(msg['k']['c']))

        data['High'] = data['High'].replace(
            data.iloc[-1]['High'], float(msg['k']['h']))

        data['Low'] = data['Low'].replace(
            data.iloc[-1]['Low'], float(msg['k']['l']))

        data['Volume'] = data['Volume'].replace(
            data.iloc[-1]['Volume'], float(msg['k']['v']))

        data['Quote_Volume'] = data['Quote_Volume'].replace(
            data.iloc[-1]['Quote_Volume'], float(msg['k']['q']))

        setDatafFame()

    else:
        print(check)
        data = data.append({
            'Date': time,
            'High': high,
            'Low': low,
            'Close': close,
            'Open': open,
            'Volume': volume,
            'Quote_Volume': qVolume,
        }, ignore_index=True)
        setDatafFame()
