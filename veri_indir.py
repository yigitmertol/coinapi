## COIN CAP API
import requests
import pandas as pd
import numpy as np
import datetime
import os

#Time now
import time
now = time.localtime()


# Yigit's functions
import sys
sys.path.append('../../Time_Series_Forecasting/TS_helpers/')
from dates_coinapi import make_all_time_indexes, make_integer_time_index

api_key = "B90AC06B-4BCB-4FC2-B3BA-CBBABCF63791"


# URL and headers for coin api account
url = 'https://rest.coinapi.io/v1/trades/BITSTAMP_SPOT_BTC_USD/history?'
headers = {'X-CoinAPI-Key' : api_key}


# Directory where data is read from and written to
data_dir = '../../../Data/Coins/BTC_USD/Trade_Data/'

df_saved = pd.read_csv( data_dir + 'BITSTAMP_TRADE_SPOTBTC_last_saved_trade.csv' , index_col='uuid')
df_original = pd.read_csv( data_dir + 'BITSTAMP_TRADE_SPOTBTC_cum.csv', nrows=1 , index_col='uuid')

# Manual day addition until above code fixed

first_day_of_new = df_saved['time_exchange'].max()
if first_day_of_new[-9:-1] != '.9999999':
    next_time = str(int(first_day_of_new[-8:-1]) + 1)
    first_day_of_new = first_day_of_new[:-8] + "0" * int(len('0000000') - len(next_time)) + next_time + 'Z'
else:
    None

last_day_of_new = '2018-01-01'

url = 'https://rest.coinapi.io/v1/trades/BITSTAMP_SPOT_BTC_USD/history?time_start='+ first_day_of_new + '&time_end=' + last_day_of_new + '&limit=100000'

# # url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?period_id=1MIN&time_start=2016-01-01T00:00:00'

response = requests.get(url, headers=headers)


if response.content[2:7] != b'error':
    limit = True
    df_new = pd.read_json(response.content).set_index('uuid')
    print('\n\nIndirilen yeni veri sayisi: ', df_new.shape[0])
else:
    print('\n\nGunluk limit dolu.\n\n')
    limit = False
    df_saved.to_csv(data_dir +'/Ben_yokken_inenler/' + 'last_' + '-'.join([str(x) for x in now]) + '_.csv')


if limit:
    df_new = make_all_time_indexes(df_new )
    df_new = df_new[df_original.columns.values]
    # print(df_saved['time_exchange'].unique().max())
    # print(df_new['time_exchange'].unique().min())
    # print(df_new['time_exchange'].unique().max())

    df_new.tail(1).to_csv(data_dir + 'BITSTAMP_TRADE_SPOTBTC_last_saved_trade.csv')


    with open(data_dir + 'BITSTAMP_TRADE_SPOTBTC_cum.csv', 'a') as fd:
        df_new.to_csv(fd, header=False)

    
    df_new.to_csv(data_dir+'/Ben_yokken_inenler/' + 'newdata_' + '-'.join([str(x) for x in now]) + '_.csv')


    print('\n\n '+ str(df_new.shape[0]) + ' veri kaydedildi.')


