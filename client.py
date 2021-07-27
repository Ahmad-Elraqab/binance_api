from config import API_KEY, API_SECRET, exchange_pairs, telegram_bot_id, telegram_chat_id
from binance import ThreadedWebsocketManager
from binance import Client
from decimal import Decimal
import pandas as pd
import numpy as np
import requests

# STREAM API
kilne_tracker = {}
data = pd.DataFrame(kilne_tracker)

twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
twm.start()

def handle_socket_message(msg):
    value = float(msg['k']['c']) / float(msg['k']['o'])
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    if symbol in kilne_tracker:
        if time in kilne_tracker[symbol]:
            kilne_tracker[symbol][time] = value

        elif (value >= exchange_pairs[symbol]['rate']):
            kilne_tracker[symbol][time] = value
            print(value)
            message = "THIS " + symbol + " HAS INCREASED BY " + str(value)
            send_message(message)
        else:
            kilne_tracker[symbol][time] = value
            print(value)

    else:

        kilne_tracker[symbol] = {}


def get_url(url, data):
    url = "https://api.telegram.org/bot1805612273:AAGq8OiJBDy1ktg-4MGEVj-7r4H3jE_2nis/sendMessage?chat_id=" + \
        telegram_chat_id+"&text="+data

    requests.request("GET", url)


def send_message(text):
    url = "https://api.telegram.org/" + telegram_bot_id + "/sendMessage"
    get_url(url, text)


for pair in exchange_pairs:
    twm.start_kline_socket(callback=handle_socket_message, symbol=pair)

twm.join()