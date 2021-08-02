from config import exchange_pairs, telegram_bot_id, telegram_chat_id
import pandas as pd
import requests

# STREAM API


kilne_tracker = {}
data = pd.DataFrame(kilne_tracker)
current_volume = 0


def handle_socket_message(msg):
    # print(msg)
    current_volume = float(msg['k']['q'])
    symbol = msg['s']

    if symbol in kilne_tracker:
        value = kilne_tracker[symbol]
        kilne_tracker[symbol] = current_volume

        if current_volume > 3 * value:
            print(current_volume)

    else:
        kilne_tracker[symbol] = current_volume


def handle_socket_message_30m(msg):
    value = float(msg['k']['q'])
    time = pd.to_datetime(msg['k']['t'], unit='ms')
    symbol = msg['s']

    if symbol in kilne_tracker:
        if time in kilne_tracker[symbol]:
            kilne_tracker[symbol][time] = value

        else:
            getVolume(symbol=symbol)
            kilne_tracker[symbol][time] = value
    else:

        kilne_tracker[symbol] = {}


def getVolume(symbol):
    # print(list(kilne_tracker[symbol].values())[-1])
    # print(len(list(kilne_tracker[symbol].values())))
    data = list(kilne_tracker[symbol].values())

    if len(data) >= 2:

        if data[-1] > 3 * data[-2]:
            message = symbol + " = " + str(data[-1])
            print(data[-1])
            send_message(message)


# def handle_socket_message(msg):
#     value = float(msg['k']['c']) / float(msg['k']['o'])
#     time = pd.to_datetime(msg['k']['t'], unit='ms')
#     symbol = msg['s']

#     if symbol in kilne_tracker:
#         if time in kilne_tracker[symbol]:
#             kilne_tracker[symbol][time] = value

#         elif (value >= exchange_pairs[symbol]['rate']):
#             kilne_tracker[symbol][time] = value
#             print(value)
#             message = symbol + " = " + str(value)
#             send_message(message)
#         else:
#             kilne_tracker[symbol][time] = value
#             print(value)

#     else:

#         kilne_tracker[symbol] = {}


def get_url(url, data):
    url = "https://api.telegram.org/bot1805612273:AAGq8OiJBDy1ktg-4MGEVj-7r4H3jE_2nis/sendMessage?chat_id=" + \
        telegram_chat_id+"&text="+data

    requests.request("GET", url)


def send_message(text):
    url = "https://api.telegram.org/" + telegram_bot_id + "/sendMessage"
    get_url(url, text)
