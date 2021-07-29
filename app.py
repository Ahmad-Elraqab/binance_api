from binance.client import Client
from config import API_KEY, API_SECRET, exchange_pairs
from binance.streams import ThreadedWebsocketManager
from client import handle_socket_message
import pandas as pd
from chart_analyzer import getData, analyzePoint, loadDate, setOrder, tech_1

twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)

twm.start()


loadDate("5m")
setOrder()

# def handle_symbol_price(msg):
#     # print(msg)0
#     # analyzePoint("30m", msg['data']['s'], msg['data']['i'])
#     tech_1("30m", msg['data']['s'], msg['data']['i'])



# for pair in exchange_pairs:

#     twm.start_symbol_mark_price_socket(
#         callback=handle_symbol_price, symbol=pair)
#     twm.start_kline_socket(callback=handle_socket_message, symbol=pair)


# twm.join()


