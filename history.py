import threading
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from config import API_KEY, API_SECRET, exchange_pairs
import pandas as pd
import time
import os
import errno


kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)

INTERVAL = '15m'
H_HISTORY = Client.KLINE_INTERVAL_15MINUTE


def readHistory():

    print('start reading history of ' +
          str(len(exchange_pairs)) + ' USDT pairs...')

    for i in exchange_pairs:

        try:

            try:
                os.makedirs(f"stream/" + i)
            except:
                pass

            klines = client.get_historical_klines(
                symbol=i, interval=H_HISTORY, start_str="1 hours ago")

            data = pd.DataFrame(klines)

            data[0] = pd.to_datetime(data[0], unit='ms')

            data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                            'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

            data = data.drop(columns=['IGNORE',
                                      'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

            # data = data.set_index('Date')
            data['diff_p'] = None
            data['diff_v'] = None
            data['Close'] = pd.to_numeric(
                data['Close'], errors='coerce')

            kilne_tracker[i] = data

            print(i + ' is loaded...')

        except:

            print(i + ' caused error')

    print('Done.')
    print('Start Streaming ---- RSI ----.')


def updateFrame(symbol, msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    check = np.where(
        kilne_tracker[symbol].iloc[-1]['Date'] == time, True, False)

    if check == True:

        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Open')] = float(msg['k']['o'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('High')] = float(msg['k']['h'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Low')] = float(msg['k']['l'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Close')] = float(msg['k']['c'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Volume')] = float(msg['k']['v'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('Quote_Volume')] = float(msg['k']['q'])
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('diff_p')] = (
            float(msg['k']['c']) - float(msg['k']['o'])) / float(msg['k']['c']) * 100
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('diff_v')] = (float(msg['k']['v']) - float(
            kilne_tracker[symbol].iloc[-2, kilne_tracker[symbol].columns.get_loc('Volume')])) / float(msg['k']['v']) * 100

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
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('diff_p')] = (
            float(msg['k']['c']) - float(msg['k']['o'])) / float(msg['k']['c']) * 100
        kilne_tracker[symbol].iloc[-1, kilne_tracker[symbol].columns.get_loc('diff_v')] = (float(msg['k']['v']) - float(
            kilne_tracker[symbol].iloc[-2, kilne_tracker[symbol].columns.get_loc('Volume')])) / float(msg['k']['v']) * 100

    kilne_tracker[symbol].to_csv(
        f'stream/'+symbol+'/data@'+symbol+'#'+INTERVAL+'.csv')


def handle_socket(msg):

    time = pd.to_datetime(msg['k']['t'], unit='ms')
    close = msg['k']['c']
    symbol = msg['s']

    updateFrame(symbol=symbol, msg=msg)


def realtime(msg):

    if 'data' in msg:
        handle_socket(msg['data'])

    else:
        stream.stream_error = True


class Stream():

    def start(self):
        self.bm = ThreadedWebsocketManager(
            api_key=API_KEY, api_secret=API_SECRET)
        self.bm.start()
        self.stream_error = False
        self.multiplex_list = list()

        # listOfPairings: all pairs with USDT (over 250 items in list)
        for pairing in exchange_pairs:
            self.multiplex_list.append(pairing.lower() + '@kline_'+INTERVAL)
        self.multiplex = self.bm.start_multiplex_socket(
            callback=realtime, streams=self.multiplex_list)

        # monitoring the error
        stop_trades = threading.Thread(
            target=stream.restart_stream, daemon=True)
        stop_trades.start()

    def restart_stream(self):
        while True:
            time.sleep(1)
            if self.stream_error == True:
                self.bm.stop_socket(self.multiplex)
                time.sleep(5)
                self.stream_error = False
                self.multiplex = self.bm.start_multiplex_socket(
                    callback=realtime, streams=self.multiplex_list)


readHistory()

stream = Stream()
stream.start()
stream.bm.join()
