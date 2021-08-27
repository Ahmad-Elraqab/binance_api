from orders_action import analyzeOrder, setOrder
from binance.client import AsyncClient, Client
from config import API_KEY, API_SECRET, exchange_pairs
from binance.streams import ThreadedWebsocketManager
from client import handle_socket_message, handle_socket_message_30m
import pandas as pd
from chart_analyzer import getData, analyzePoint, loadDate
from buy_zone import handle_socket_buy_zone

twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)

twm.start()

# points_list = []

# points_list = loadDate("4hr")

# setOrder(points_list)


# def handle_symbol_price(msg):
#     analyzeOrder(float(msg['data']['i']))
#     # twm.stop()


# twm.start_symbol_mark_price_socket(
#     callback=handle_symbol_price, symbol='ETHUSDT')

# for pair in exchange_pairs:
#     # twm.start_kline_socket(
#     #     callback=handle_socket_message, symbol=pair)
#     twm.start_kline_socket(
#         callback=handle_socket_message_30m, symbol=pair, interval=AsyncClient.KLINE_INTERVAL_5MINUTE)

twm.start_options_kline_socket(
    callback=handle_socket_buy_zone, symbol='BTC-200630-9000-P', interval=AsyncClient.KLINE_INTERVAL_1MINUTE)

twm.join()
