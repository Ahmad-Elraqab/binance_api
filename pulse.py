from binance.client import Client
from numpy import sqrt
import pandas as pd
from config import API_KEY, API_SECRET


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


class Node:
    def __init__(self, dataval=None):
        self.dataval = dataval
        self.nextval = None


class SLinkedList:
    def __init__(self):
        self.headval = None

    def listprint(self):
        printval = self.headval
        while printval is not None:
            print(printval.dataval)
            printval = printval.nextval

    def checkSize(self):

        head = self.headval
        count = 0

        while head.nextval is not None:

            count += 1
            head = head.nextval

        return count

    def add(self, newNode):

        head = self.headval
        size = self.checkSize()

        if size >= 4:

            head = head.nextval

        while head.nextval is not None:

            head = head.nextval

        head.nextval = newNode


client = Client(api_key=API_KEY, api_secret=API_SECRET)
klines = client.get_historical_klines(
    symbol='ETHUSDT', interval=Client.KLINE_INTERVAL_5MINUTE, start_str="15 sep 2021")

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
