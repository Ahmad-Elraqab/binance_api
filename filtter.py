from models.node import Node
from numpy import sqrt
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET
from binance.client import Client
import pandas as pd
import matplotlib.pyplot as plt
from client import send_message

def getData():

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol="ADAUSDT", interval=Client.KLINE_INTERVAL_30MINUTE, start_str="1 jul 2021")

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
