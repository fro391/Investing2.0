'''
NASDAQ stock screener website: https://www.nasdaq.com/market-activity/stocks/screener
    API: https://www.nasdaq.com/api/v1/screener?page=1&pageSize=50
YAHOO stock screener website: https://finance.yahoo.com/screener/unsaved/a6f346da-9b29-457b-87f9-36c76769db1f
    API: https://query1.finance.yahoo.com/v1/finance/screener/public/saved?formatted=true&lang=en-US&region=US&start=0&count=250&scrId=a6f346da-9b29-457b-87f9-36c76769db1f&corsDomain=finance.yahoo.com
'''
import datetime
import time
import requests

import json
import os
import pandas as pd
import numpy as np

from time import sleep

#threading
import threading
import timeit

#declare global lock object
global lock
lock = threading.Lock()

def nasdaq_ticker_downloader():
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
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
    
    for i in range(350):
        try:
            start_pos = i+1
            print (start_pos)
            url = 'https://www.nasdaq.com/api/v1/screener?page={}&pageSize=20'.format(start_pos)
            resp = requests.get(url, headers=hdr)
            if resp.status_code != 200:
                # This means something went wrong.
                raise ApiError('GET /tasks/ {}'.format(resp.status_code))
            data = json.loads(resp.text)

            #add new symbols to old list
            with open('symbols_alt.txt','r') as symbolfile:
                symbolslist = symbolfile.read().split('\n')
            len1 = len(symbolslist)
            for j in range(49):
                try:
                    symbol = data['data'][j]['ticker']
                    if symbol not in symbolslist: #add scrapped symbol into list if not already in it
                        symbolslist.append(symbol)
                except:
                    pass
            len2 = len(symbolslist)

            with open ('symbols.txt','w+') as symbolfile:             #write to old file
                counter = 0 #counter for not adding '\n' at the last line
                for s in symbolslist:
                    counter += 1
                    if counter < len(symbolslist):
                        symbolfile.write(str(s)+ '\n')
                    else:
                        symbolfile.write(str(s))
            print ("Added", len2-len1, "stocks")
        except:
            pass

def nasdaq_ticker_analyzer():
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    symbolslist = []
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
    
    for i in range(350):
        try:
            start_pos = i+1
            url = 'https://www.nasdaq.com/api/v1/screener?page={}&pageSize=20'.format(start_pos)
            resp = requests.get(url, headers=hdr)
            if resp.status_code != 200:
                # This means something went wrong.
                raise ApiError('GET /tasks/ {}'.format(resp.status_code))
            data = json.loads(resp.text)
            for j in range(19):
                try:
                    if data['data'][j]['bloggerSentimentData']['signal'] == 'Bullish' and data['data'][j]['newsSentimentData']['score'] >= 0.8:#data['data'][j]['newsSentimentData']['score'] >= 0.5 and data['data'][j]['insiderSentimentData']['score'] >= 0.5 and data['data'][j]['mediaBuzzData']['score'] >= 0.5 and data['data'][j]['hedgeFundSentimentData']['score'] >= 0.5 and data['data'][j]['investorSentimentData']['score'] >= 0.5 and data['data'][j]['bloggerSentimentData']['signal'] == 'Bullish': 
                        symbolslist.append(data['data'][j]['ticker'])
                        print(data['data'][j]['ticker'])
                        print ('newsSentimentData', data['data'][j]['newsSentimentData'])
                        print ('insiderSentimentData', data['data'][j]['insiderSentimentData'])
                        print ('mediaBuzzData', data['data'][j]['mediaBuzzData'])
                        print ('hedgeFundSentimentData', data['data'][j]['hedgeFundSentimentData'])
                        print ('investorSentimentData', data['data'][j]['investorSentimentData'])
                        print ('bloggerSentimentData', data['data'][j]['bloggerSentimentData'])
                except:
                    pass
        except:
            pass
    #add symbols to file
        with open ('symbols_nasdaq.txt','w+') as symbolfile:             
            counter = 0 #counter for not adding '\n' at the last line
            for s in symbolslist:
                counter += 1
                if counter < len(symbolslist):
                    symbolfile.write(str(s)+ '\n')
                else:
                    symbolfile.write(str(s))

        
if __name__ == '__main__':
    #start timer
    start = timeit.default_timer()
    nasdaq_ticker_downloader()
    nasdaq_ticker_analyzer()
    
