import datetime
import time
import requests

import json
import os
import pandas as pd
import numpy as np

#threading
import threading
import timeit

#declare global lock object
global lock
lock = threading.Lock()

def symbol_downloader(symbol, directory, days=10000, days_ago=0):
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    #web variables
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/'+symbol+'?period1='+str(UnixTime-86400*(days+days_ago))+'&period2='+str(UnixTime-86400*days_ago)+'&interval=1d&indicators=quote%7Csma~50&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-CA&region=CA&corsDomain=ca.finance.yahoo.com'
    #proxies
    http_proxy  = ''
    https_proxy = ''
    ftp_proxy   = ''
    proxyDict = { 
                  "http"  : http_proxy, 
                  "https" : https_proxy, 
                  "ftp"   : ftp_proxy
                }

    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
       'Connection': 'keep-alive'}
    
    try:
        resp = requests.get(url, headers=hdr)
        if resp.status_code != 200:
            # This means something went wrong.
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        data = json.loads(resp.text)
        #json unpack
        timestamp = data['chart']['result'][0]['timestamp']
        timestamp = [datetime.datetime.fromtimestamp(x).strftime('%Y%m%d') for x in timestamp]
        quote = data['chart']['result'][0]['indicators']['quote'][0]
        sma50 = data['chart']['result'][0]['indicators']['sma'][0]['sma']
        stock_df = pd.DataFrame(quote)
        #index is symbol and timestamp
        stock_df.index = [str(x) for x in timestamp]

        #moving averages
        stock_df['sma'] = sma50
        stock_df['vol20'] = stock_df['volume'].rolling(window=20).mean()
        #predict on yesterday's averages
        stock_df['sma'] = stock_df['sma'].shift(1)
        stock_df['vol20'] = stock_df['vol20'].shift(1)
        stock_df = stock_df.dropna()

        #derived columns
        stock_df['close'] = stock_df['close']/stock_df['sma']
        stock_df['high'] = stock_df['high']/stock_df['sma']
        stock_df['low'] = stock_df['low']/stock_df['sma']
        stock_df['open'] = stock_df['open']/stock_df['sma']
        stock_df['volume'] = stock_df['volume']/stock_df['vol20']

        #stock_df.drop(['close', 'high', 'low', 'open','volume','sma','vol20'], axis=1, inplace=True)
        stock_df = stock_df.dropna()
        stock_df = stock_df[~(stock_df == np.inf).any(axis=1)]
        if len(stock_df) != 0:
            stock_df.to_csv('{}{}.csv'.format(directory,symbol[:4]))
        
    except Exception as ex:
        pass

if __name__ == '__main__':

    #start timer
    start = timeit.default_timer()

    symbolslist = open('symbols_alt.txt').read().split('\n')

    directory = './data_nasdaq/'

    threadlist = []

    for u in symbolslist:

        t = threading.Thread(target = symbol_downloader,args=(u,directory))
        t.start()
        threadlist.append(t)
        #sets top limit of active threads to 50
        while threading.activeCount()>50:
            a=0
        #print threading.activeCount()

    for b in threadlist:
        b.join()
    print ("# of threads: ", len(threadlist))
    
    directory = './data_nasdaq/'
    # get list with filenames in folder and throw away all non ncsv
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    
    for f in files:
        try:
            stock_df = pd.read_csv(os.path.join(directory, f))

            if float(stock_df.tail(1)['close']) >= 1.1 and float(stock_df.tail(1)['volume']) >= 1 and float(stock_df.tail(1)['sma']) <= 20 and float(stock_df.tail(1)['sma']) >= 2:

                print ('{} has high price and volume movement on {}, and is under $10'.format(f,stock_df.index[-1]))

        except: 
            pass

    #timer
    stop = timeit.default_timer()
    print ("seconds of operation: " , stop - start)
