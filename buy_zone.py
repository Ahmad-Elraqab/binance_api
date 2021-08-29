from models.node import Node
from numpy import sqrt
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET
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


def getData():

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol="BTCUSDT", interval=Client.KLINE_INTERVAL_30MINUTE, start_str="1 jul 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['High', 'Low', 'IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    col_zscore = 'zscore'
    close = pd.to_numeric(data['Close'])
    volume = pd.to_numeric(data['Volume'])

    mean = (volume*close).rolling(window=48).sum() / \
        volume.rolling(window=48).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=48).mean())
    data[col_zscore] = (close-mean)/vwapsd

    df = pd.DataFrame(columns=['symbol',
                               'interval',
                               'price',
                               'buyAmount',
                               'gainProfit',
                               'gainAmount',
                               'totalAmount',
                               'startDate',
                               'endDate',
                               'volume',
                               'quoteVolume'
                               ])

    for index, row in data.iterrows():

        if row[col_zscore] <= -2.5:
            ordersList.append(
                Order(symbol='BTCUSDT', interval='30M', price=row['Close'], amount=500, startDate=row['Date'], volume=row['Volume'], qVolume=row['Quote_Volume']))
        else:
            for order in ordersList:

                rate = ((row['Close'] - order.price) / order.price) * 100

                if order.sellProfit == True:

                    if rate >= 5.0:
                        order.price = row['Close']
                        order.gainProfit += rate

                    elif rate <= -2.5 and order.price > row['Close']:
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
                                   'volume': order.volume,
                                   'quoteVolume': order.qVolume,
                                   }
                        df = df.append(new_row, ignore_index=True)
                        ordersList.remove(order)

                elif row[col_zscore] >= 2.0 and rate > 0:
                    order.sellProfit = True
                    order.price = row['Close']
                    order.gainProfit += rate

    print(len(ordersList))
    df.to_csv(f'files/data.csv', index=False,
              header=True, mode='a')


getData()


def handle_socket_buy_zone(data):
    print(data)
