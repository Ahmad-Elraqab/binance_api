from config import API_KEY, API_SECRET
from binance.client import Client
import pandas as pd

def getData(symbol):

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol=symbol, interval=Client.KLINE_INTERVAL_30MINUTE, start_str="1 aug 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    data['volatility ratio'] = (data['High'] - data['Low']) / \
        (
        data['High'].rolling(14).max() -
        data['Low'].rolling(14).min())

    data['volatility ratio'].plot()
    


    data.to_csv(f'files/data2.csv', index=False,
                header=True, mode='a')