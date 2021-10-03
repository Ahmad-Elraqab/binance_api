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

ordersList = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)

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


def setDatafFame(symbol):

    close = pd.to_numeric(kilne_tracker[symbol]['Close'])

    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    kilne_tracker[symbol]['48-zscore'] = zScore(
        window=48, close=close, volume=volume)

    kilne_tracker[symbol].to_csv(
        f'files/'+symbol+'@data.csv', index=False, header=True)


def readHistory():
    print('start reading history of ' +
          str(len(exchange_pairs)) + ' USDT pairs...')

    for i in exchange_pairs:

        try:

            klines = client.get_historical_klines(
                symbol=i, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="1 days ago")

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

        except:

            print(i + ' caused error')


def sell(time):

    list = ordersList
    for key, value in list.items():

        try:
            symbol = value.symbol
            rate = ((float(kilne_tracker[symbol].iloc[-1]['Close']) - float(value.price[-1])) /
                    float(value.price[-1])) * 100

            # if kilne_tracker[symbol].iloc[-1]['48-zscore'] >= -1.0 and rate > 0.0:
            if rate >= 3.0:
                value.gainProfit += rate
                value.endDate = kilne_tracker[symbol].iloc[-1]['Date']
                value.price.append(kilne_tracker[symbol].iloc[-1]['Close'])
                value.sellList.append(
                    kilne_tracker[symbol].iloc[-1]['Date'])
                value.sellBlack = pd.to_numeric(
                    kilne_tracker[symbol].iloc[-1]['48-zscore'])

                new_row = {'symbol': value.symbol,
                           'type': value.type,
                           'interval': value.interval,
                           'buyPrice': value.buyPrice,
                           'sellPrice': value.price,
                           'buyAmount': value.amount,
                           'gainProfit': value.gainProfit,
                           'gainAmount': value.gainProfit / 100 * value.amount,
                           'totalAmount': (value.gainProfit / 100 + 1) * value.amount,
                           'startDate': value.startDate,
                           'endDate': value.endDate,
                           'sellVolume': value.sellVolume,
                           'volume': value.volume,
                           'quoteVolume': value.qVolume,
                           'sellList': value.sellList,
                           'buyBlack': 0,
                           'buyRed': 0,
                           'buyBlue': 0,
                           'buyRatio': 0,
                           'sellBlack': 0,
                           'sellRed': 0,
                           'sellBlue': 0,
                           'sellRatio': 0,
                           }
                message = ' اغلاق صفقة ' + str(symbol) + ' من تاريخ ' + str(value.startDate) + \
                    ' على سعر ' + str(kilne_tracker[symbol].iloc[-1]['Close']) + ' بتاريخ ' + \
                    str(time) + ' بربح ' + str(rate)
                send_message(message)
                print('sell ' + symbol)

                ordersList[key] = None
                global df
                df = df.append(
                    new_row, ignore_index=True)
                df.to_csv(f'files/data2.csv', index=False,
                          header=True, mode='a')
        except:
            print('')


def buy(symbol, time):

    # try:
    if kilne_tracker[symbol].iloc[-2]['48-zscore'] <= -2.35 and time not in ordersList:
        ordersList[time] = Order(symbol=symbol, type='48',
                                 interval='5m',
                                 buyPrice=kilne_tracker[symbol].iloc[-2]['Close'],
                                 price=[
                                     kilne_tracker[symbol].iloc[-2]['Close']],
                                 amount=500,
                                 startDate=pd.to_datetime(
                                     kilne_tracker[symbol].iloc[-2]['Date']),
                                 volume=kilne_tracker[symbol].iloc[-2]['Volume'],
                                 qVolume=kilne_tracker[symbol].iloc[-2]['Quote_Volume'],
                                 buyBlack=pd.to_numeric(
                                     kilne_tracker[symbol].iloc[-2]['48-zscore']),
                                 buyRed=0,
                                 buyBlue=0,
                                 buyRatio=0
                                 )

        message = ' شراء عملة ' + symbol+' على سعر ' + \
            str(kilne_tracker[symbol].iloc[-2]['Close']) + ' بتاريخ ' + \
            str(pd.to_datetime(kilne_tracker[symbol].iloc[-2]['Date'])) + \
            ' zscore ' + str(kilne_tracker[symbol].iloc[-2]['48-zscore'])
        print('buy ' + symbol)
        send_message(message)
    # except:
    #     print('Error while buying...')


def updateFrame(symbol, msg):

    # try:
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    check = np.where(
        kilne_tracker[symbol].iloc[-1]['Date'] == time, True, False)

    if check == True:

        
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Open')] =  float(msg['k']['o'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('High')] =  float(msg['k']['h'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Low')] =  float(msg['k']['l'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Close')] =  float(msg['k']['c'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Volume')] =  float(msg['k']['v'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Quote_Volume')] =  float(msg['k']['q'])

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

        buy(symbol, time)
    setDatafFame(symbol=symbol)

    # except:

    #     print('Error while updating data...')


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    updateFrame(symbol, msg)

    for i in ordersList.keys():

        if i == None:

            # print(i)
            ordersList.pop(i)

        # else:/
            # print(i)

    sell(symbol=symbol, time=time)


# for pair in exchange_pairs:

#     try:
#         getData(symbol=pair)
#     except:
#         print(pair + ' caused error')
