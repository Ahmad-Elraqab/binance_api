from binance.client import Client
from config import API_KEY, API_SECRET, exchange_pairs
from binance.streams import ThreadedWebsocketManager
from client import handle_socket_message
import pandas as pd

twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)

twm.start()

def handle_symbol_price(msg):
    print(msg)

for pair in exchange_pairs:
    twm.start_symbol_mark_price_socket(callback=handle_symbol_price, symbol=pair)
    twm.start_kline_socket(callback=handle_socket_message, symbol=pair)
    # data = client.get_ticker(params=pair)
    # print(pair, '\t\t\t', data)
    # twm.start_options_kline_socket


twm.join()


