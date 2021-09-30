from binance.client import Client
from numpy import sqrt
import numpy
import pandas as pd
from config import API_KEY, API_SECRET


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


client = Client(api_key=API_KEY, api_secret=API_SECRET)
klines = client.get_historical_klines(
    symbol='BTCUSDT', interval=Client.KLINE_INTERVAL_30MINUTE, start_str="1 aug 2021")

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


def outlier_treatment(datacolumn):

    for key, value in enumerate(datacolumn):
        # print(key)
        # print(value)
        dataframe = sorted(datacolumn[key: key+15])
        Q1 = numpy.percentile(dataframe, 25, interpolation='midpoint')
        Q2 = numpy.percentile(dataframe, 50, interpolation='midpoint')
        Q3 = numpy.percentile(dataframe, 75, interpolation='midpoint')
        IQR = Q3 - Q1
        lower_range = Q1 - (1.5 * IQR)
        upper_range = Q3 + (1.5 * IQR)

        data['IQR'] = IQR
        data['lower_range'] = lower_range
        data['upper_range'] = upper_range

    # print(str(lower_range) + '------' + str(upper_range))
    # return lower_range, upper_range
    data.to_csv(f'files/data.csv', index=False, header=True)


outlier_treatment(data['Close'])
# data['Q1'], data['Q3'] = outlier_treatment(data['Close'].rolling(15).)
