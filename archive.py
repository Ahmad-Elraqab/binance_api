# from binance.client import Client
# API_KEY = 'yo3Wx8sTYwGt9Lz5f3vuAiaosy7esnAxCjFnDrmmqq0NdLZ0pk5RQl4aK91lmO6w'
# API_SECRET = 'ldJo4Hw0rLUDPX8P5zm5d3TYsvJYLA5ikm7GEDhSb33xSMFcDrF8u7rRGZLvwr7s'


# @@ GET USDT PAIRS @@

# exchange_pairs = {}
# client = Client(api_key=API_KEY, api_secret=API_SECRET)
# exchange_info = client.get_exchange_info()
# for s in exchange_info['symbols']:
#     if 'USDT' in s['symbol']:
#         exchange_pairs[s['symbol']] = {'rate': 1.5}
# print(exchange_pairs)


# @@ GET HISTORICAL DATA @@

# CLient data
# client = Client(api_key=API_KEY, api_secret=API_SECRET)
# data = client.get_historical_klines('BTCUSDT', client.KLINE_INTERVAL_4HOUR, "21 july 2021")
# data = pd.DataFrame(data)
# if not data.empty:
# 	data[0] =  pd.to_datetime(data[0],unit='ms')
# 	data.columns = ['Date','Open','High','Low','Close','Volume','IGNORE','Quote_Volume','Trades_Count','BUY_VOL','BUY_VOL_VAL','x']
# 	data = data.set_index('Date')
# 	del data['IGNORE']
# 	del data['BUY_VOL']
# 	del data['BUY_VOL_VAL']
# 	del data['x']
# 	data["Open"] = pd.to_numeric(data["Open"])
# 	data ["Open"] = pd.to_numeric(data["Open"])
# 	data ["High"] = pd.to_numeric(data["High"])
# 	data ["Low"] = pd.to_numeric(data["Low"])
# 	data ["Close"] = pd.to_numeric(data["Close"])
# 	data ["Volume"] = round(pd.to_numeric(data["Volume"]))
# 	data ["Quote_Volume"] = round(pd.to_numeric(data["Quote_Volume"]))
# 	data ["Trades_Count"] = pd.to_numeric(data["Trades_Count"])
# 	data['div']= data['Open'] / data['Close']
# 	data['Log_VolumeGain'] = (np.log(data["Quote_Volume"]/data.Quote_Volume.shift(1))*100).fillna(0)
# 	data['pricegain'] = (data.Open.pct_change()*100).fillna(0)
# 	data.to_csv(f'files/BTCUSDT.csv')
# print(data)


# @@ OPEN MULTIPLE SOCKETS @@

# multiple sockets can be started
# twm.start_depth_socket(callback=handle_socket_message, symbol=symbol)
# or a multiplex socket can be started like this
# see Binance docs for stream names
# streams = ['bnbbtc@miniTicker', 'bnbbtc@bookTicker']
# streams = ['bnbbtc@kline_1m']
# twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)


# @@ PRINT IN FILE @@

# def printInFile():
# with open('data.csv', mode='w') as csv_file:
# fieldnames = ['PAIR\t\t\t', 'RATE\t']
# writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
# writer.writeheader()
# for key, value in kilne_tracker.items():
# if(len(value) != 0):
# if(list(value.values())[-1] >= exchange_pairs[key]['rate']):
# # print(key, '\t\t', list(value.values())[-1])
# value = "THIS " + key + " HAS INCREASED BY " + \
# str(list(value.values())[-1])
# send_message(value)
# writer.writerow(
# {'PAIR\t\t\t': key+'\t\t\t', 'RATE\t': list(value.values())[-1]})
# else:
# continue

# if row['48-zscore'] >= 2.5:
#     order.gainProfit += rate
#     order.endDate = row['Date']
#     new_row = {'symbol': order.symbol,
#                'interval': order.interval,
#                'price': 'null',
#                'buyAmount': order.amount,
#                'gainProfit': order.gainProfit,
#                'gainAmount': order.gainProfit / 100 * order.amount,
#                'totalAmount': (order.gainProfit / 100 + 1) * order.amount,
#                'startDate': order.startDate,
#                'endDate': order.endDate,
#                'avgDate': order.endDate - order.startDate,
#                'volume': order.volume,
#                'quoteVolume': order.qVolume,
#                }
#     df = df.append(new_row, ignore_index=True)
#     ordersList.remove(order)
