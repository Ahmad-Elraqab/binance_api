from numpy import sqrt
import numpy
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET
from binance.client import Client
import pandas as pd
import matplotlib.pyplot as plt
from client import send_message
from models.order import Order

ordersList = []


def getData():

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol="BTCUSDT", interval=Client.KLINE_INTERVAL_30MINUTE, start_str="1 aug 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    # data = data.set_index('Date')
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

    for index, row in data.iterrows():

        if row[col_zscore] <= -3:
            
            ordersList.append(
                Order(symbol='BTCUSDT', interval='30M', price=row['Close'], stopLose=0, sellPercent=0))
            send_message('buy at' + str(row['Close']))

        for order in ordersList:
            if (row['Close'] - order.price) / order.price * 100 >= 2.5:
                order.price = row['Close']
                send_message('sell profit of order at' +
                             str(row['Close']) + ' on ' + str(row['Date']))

            if (row['Close'] - order.price) / order.price * 100 <= -2.5:
                ordersList.remove(order)
                send_message('close orders at' +
                             str(row['Close']) + ' on ' + str(row['Date']))

                # df.to_csv(f'files/data.csv', index=False, header=True, mode='a')
                # data['zscore'].plot()
                # print(data)
                # plt.show()


getData()


def handle_socket_buy_zone(data):
    print(data)
