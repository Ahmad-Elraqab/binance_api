from models.node import Node
from numpy import sqrt
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET, exchange_pairs
from binance.client import Client
import pandas as pd
import matplotlib.pyplot as plt
from client import send_message

ordersList = []


class Order:
    def __init__(self, symbol, interval, price, amount, startDate, volume, qVolume):
        self.price = price
        self.stopLose = False
        self.sellProfit = False
        self.symbol = symbol
        self.interval = interval
        self.gainProfit = 0
        self.amount = amount
        self.startDate = startDate
        self.endDate = None
        self.volume = volume
        self.qVolume = qVolume


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2.25).rolling(window=window).mean())

    return (close-mean)/vwapsd


def getData(symbol):

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="1 jul 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['High', 'Low', 'IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    close = pd.to_numeric(data['Close'])
    volume = pd.to_numeric(data['Volume'])

    data['48-zscore'] = zScore(window=48, close=close, volume=volume)
    data['199-zscore'] = zScore(window=199, close=close, volume=volume)
    data['484-zscore'] = zScore(window=484, close=close, volume=volume)

    df = pd.DataFrame(columns=['symbol',
                               'interval',
                               'price',
                               'buyAmount',
                               'gainProfit',
                               'gainAmount',
                               'totalAmount',
                               'startDate',
                               'endDate',
                               'avgDate',
                               'volume',
                               'quoteVolume'
                               ])

    for index, row in data.iterrows():

        if row['199-zscore'] <= -2.5 or row['484-zscore'] <= -2.5:
            ordersList.append(
                Order(symbol=symbol, interval='5M', price=row['Close'], amount=500, startDate=row['Date'], volume=row['Volume'], qVolume=row['Quote_Volume']))
        else:
            for order in ordersList:

                rate = ((row['Close'] - order.price) / order.price) * 100

                if row['48-zscore'] >= 1.75:
                    order.gainProfit += rate
                    order.endDate = row['Date']
                    new_row = {'symbol': order.symbol,
                               'interval': order.interval,
                               'price': 'null',
                               'buyAmount': order.amount,
                               'gainProfit': order.gainProfit,
                               'gainAmount': order.gainProfit / 100 * order.amount,
                               'totalAmount': (order.gainProfit / 100 + 1) * order.amount,
                               'startDate': order.startDate,
                               'endDate': order.endDate,
                               'avgDate': order.endDate - order.startDate,
                               'volume': order.volume,
                               'quoteVolume': order.qVolume,
                               }
                    df = df.append(new_row, ignore_index=True)
                    ordersList.remove(order)

    print(len(ordersList))
    df.to_csv(f'files/'+symbol+'.csv', index=False,
              header=True, mode='a')


for key, value in enumerate(exchange_pairs):
    getData(symbol=value)


def handle_socket_buy_zone(data):
    print(data)
