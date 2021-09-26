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
# klines = client.get_historical_klines(
#     symbol='DOGEUSDT', interval=Client.KLINE_INTERVAL_5MINUTE, start_str="2 days ago")

# data = pd.DataFrame(klines)

# data[0] = pd.to_datetime(data[0], unit='ms')


# data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
#                 'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

# data = data.drop(columns=['IGNORE',
#                           'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

# data['Close'] = pd.to_numeric(
#     data['Close'], errors='coerce')


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
    # open = pd.to_numeric(kilne_tracker[symbol]['Open'])
    # high = pd.to_numeric(kilne_tracker[symbol]['High'])
    # low = pd.to_numeric(kilne_tracker[symbol]['Low'])
    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    kilne_tracker[symbol]['48-zscore'] = zScore(
        window=48, close=close, volume=volume)
    kilne_tracker[symbol]['199-zscore'] = zScore(
        window=199, close=close, volume=volume)
    kilne_tracker[symbol]['484-zscore'] = zScore(
        window=484, close=close, volume=volume)

    # a = kilne_tracker[symbol]['High'].shift(1).rolling(14).max()
    # b = kilne_tracker[symbol]['High'].shift(1).rolling(15).max()
    # c = kilne_tracker[symbol]['Low'].shift(1).rolling(14).min()
    # d = kilne_tracker[symbol]['Low'].shift(1).rolling(15).min()

    # l = kilne_tracker[symbol]['Close'].shift(15)
    # max = DataFrame(columns=['max'], data=np.where(a < b, l, a))
    # min = DataFrame(columns=['min'], data=np.where(c > d, l, c))

    # kilne_tracker[symbol]['Max'] = max
    # kilne_tracker[symbol]['Min'] = min
    # kilne_tracker[symbol]['volatility ratio'] = (
    #     high - low) / (kilne_tracker[symbol]['Max'] - kilne_tracker[symbol]['Min'])

    # print(kilne_tracker[symbol])
    kilne_tracker[symbol].to_csv(
        f'files/'+symbol+'@data.csv', index=False, header=True)


def readHistory():
    print('start reading history of ' +
          str(len(exchange_pairs)) + ' USDT pairs...')

    for i in exchange_pairs:

        try:

            klines = client.get_historical_klines(
                symbol=i, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="2 days ago")

            data = pd.DataFrame(klines)

            data[0] = pd.to_datetime(data[0], unit='ms')

            data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                            'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

            data = data.drop(columns=['IGNORE',
                                      'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

            data['Close'] = pd.to_numeric(
                data['Close'], errors='coerce')

            kilne_tracker[i] = data

        except:

            print(i + ' caused error')


def sell(symbol, time, close):

    try:
        list = ordersList
        for key, value in list.items():

            if key == symbol:
                rate = ((kilne_tracker[symbol].iloc[-1]['Close'] - value.price[-1]) /
                        value.price[-1]) * 100

                # if kilne_tracker[symbol].iloc[-1]['48-zscore'] >= -1.0 and rate > 0.0:
                if rate >= 2.5:
                    value.gainProfit += rate
                    value.endDate = kilne_tracker[symbol].iloc[-1]['Date']
                    value.price.append(kilne_tracker[symbol].iloc[-1]['Close'])
                    value.sellList.append(
                        kilne_tracker[symbol].iloc[-1]['Date'])
                    value.sellBlack = pd.to_numeric(
                        kilne_tracker[symbol].iloc[-1]['48-zscore'])
                    value.sellRed = pd.to_numeric(
                        kilne_tracker[symbol].iloc[-1]['484-zscore'])
                    value.sellBlue = pd.to_numeric(
                        kilne_tracker[symbol].iloc[-1]['199-zscore'])
                    value.sellRatio = pd.to_numeric(
                        kilne_tracker[symbol].iloc[-1]['volatility ratio'])

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
                               'buyBlack': value.buyBlack,
                               'buyRed': value.buyRed,
                               'buyBlue': value.buyBlue,
                               'buyRatio': value.buyRatio,
                               'sellBlack': value.sellBlack,
                               'sellRed': value.sellRed,
                               'sellBlue': value.sellBlue,
                               'sellRatio': value.sellRatio
                               }
                    message = ' اغلاق صفقة ' + str(symbol) + ' من تاريخ ' + str(value.startDate) + \
                        ' على سعر ' + str(close) + ' بتاريخ ' + \
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
        print('Error')


def handle_socket(msg):

    try:
        for i in ordersList.keys():

            if i == None:

                ordersList.pop(i)

        volume = float(msg['k']['v'])
        qVolume = float(msg['k']['q'])
        time = pd.to_datetime(msg['k']['t'], unit='ms')
        close = float(msg['k']['c'])
        open = float(msg['k']['o'])
        high = float(msg['k']['h'])
        low = float(msg['k']['l'])
        symbol = msg['s']

        # print(str(time) + '   ----   ' + str(close))
        check = np.where(
            kilne_tracker[symbol].iloc[-1][0] == time, True, False)

        if check == True:

            # print(check)
            kilne_tracker[symbol]['Open'] = kilne_tracker[symbol]['Open'].replace(
                kilne_tracker[symbol].iloc[-1]['Open'], float(msg['k']['o']))

            kilne_tracker[symbol]['Close'] = kilne_tracker[symbol]['Close'].replace(
                kilne_tracker[symbol].iloc[-1]['Close'], float(msg['k']['c']))

            kilne_tracker[symbol]['High'] = kilne_tracker[symbol]['High'].replace(
                kilne_tracker[symbol].iloc[-1]['High'], float(msg['k']['h']))

            kilne_tracker[symbol]['Low'] = kilne_tracker[symbol]['Low'].replace(
                kilne_tracker[symbol].iloc[-1]['Low'], float(msg['k']['l']))

            kilne_tracker[symbol]['Volume'] = kilne_tracker[symbol]['Volume'].replace(
                kilne_tracker[symbol].iloc[-1]['Volume'], float(msg['k']['v']))

            kilne_tracker[symbol]['Quote_Volume'] = kilne_tracker[symbol]['Quote_Volume'].replace(
                kilne_tracker[symbol].iloc[-1]['Quote_Volume'], float(msg['k']['q']))

            setDatafFame(symbol=symbol)

        else:
            if kilne_tracker[symbol].iloc[-1]['48-zscore'] <= -2.35 and time not in ordersList:
                ordersList[time] = Order(symbol=symbol, type='48',
                                         interval='5m',
                                         buyPrice=kilne_tracker[symbol].iloc[-1]['Close'],
                                         price=[
                                             kilne_tracker[symbol].iloc[-1]['Close']],
                                         amount=500,
                                         startDate=kilne_tracker[symbol].iloc[-1]['Date'],
                                         volume=kilne_tracker[symbol].iloc[-1]['Volume'],
                                         qVolume=kilne_tracker[symbol].iloc[-1]['Quote_Volume'],
                                         buyBlack=pd.to_numeric(
                                             kilne_tracker[symbol].iloc[-1]['48-zscore']),
                                         buyRed=pd.to_numeric(
                                             kilne_tracker[symbol].iloc[-1]['484-zscore']),
                                         buyBlue=pd.to_numeric(
                                             kilne_tracker[symbol].iloc[-1]['199-zscore']),
                                         buyRatio=0
                                         )

                message = ' شراء عملة ' + symbol+' على سعر ' + \
                    str(close) + ' بتاريخ ' + \
                    str(kilne_tracker[symbol].iloc[-1]['Date'])
                print('buy ' + symbol)
                send_message(message)
            kilne_tracker[symbol] = kilne_tracker[symbol].append({
                'Date': time,
                'Open': open,
                'High': high,
                'Low': low,
                'Close': close,
                'Volume': volume,
                'Quote_Volume': qVolume,
            }, ignore_index=True)
            setDatafFame(symbol=symbol)

        sell(symbol=symbol, time=time, close=close)

    except:
        print('Error in data transfare')

# for pair in exchange_pairs:

#     try:
#         getData(symbol=pair)
#     except:
#         print(pair + ' caused error')
