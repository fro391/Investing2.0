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

from email.mime.text import MIMEText
import smtplib

import gc
import sys

import re

#declare global lock object
global lock
lock = threading.Lock()
def symbol_downloader(symbol, directory, days=600, days_ago=0):
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    #web variables
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/'+symbol+'?period1='+str(UnixTime-86400*(days+days_ago))+'&period2='+str(UnixTime-86400*days_ago)+'&interval=1d&indicators=quote%7Csma~60&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-CA&region=CA&corsDomain=ca.finance.yahoo.com'
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
            print(resp.status_code)
        data = json.loads(resp.text)
        #json unpack
        timestamp = data['chart']['result'][0]['timestamp']
        timestamp = [datetime.datetime.fromtimestamp(x).strftime('%Y%m%d') for x in timestamp]
        quote = data['chart']['result'][0]['indicators']['quote'][0]
        sma60 = data['chart']['result'][0]['indicators']['sma'][0]['sma']
        stock_df = pd.DataFrame(quote)
        #index is symbol and timestamp
        stock_df.index = [str(x) for x in timestamp]

        #moving averages
        stock_df['sma60'] = sma60
        stock_df['vol20'] = stock_df['volume'].rolling(window=20).mean()
        stock_df['sma5'] = stock_df['close'].rolling(window=5).mean()
        stock_df['sma8'] = stock_df['close'].rolling(window=8).mean()
        stock_df['sma13'] = stock_df['close'].rolling(window=13).mean()
        stock_df['sma21'] = stock_df['close'].rolling(window=21).mean()
        stock_df['sma34'] = stock_df['close'].rolling(window=34).mean()
        stock_df['sma55'] = stock_df['close'].rolling(window=55).mean()
        stock_df['sma89'] = stock_df['close'].rolling(window=89).mean()
        stock_df['sma144'] = stock_df['close'].rolling(window=144).mean()
        stock_df['sma233'] = stock_df['close'].rolling(window=233).mean()
        
        stock_df['ewm26'] = stock_df['close'].ewm(span=26,min_periods=0,adjust=False,ignore_na=False).mean()
        stock_df['ewm12'] = stock_df['close'].ewm(span=12,min_periods=0,adjust=False,ignore_na=False).mean()
        stock_df['MACD'] = stock_df['ewm12']-stock_df['ewm26']
        stock_df['MACD_signal'] = stock_df['MACD'].ewm(span=9,min_periods=0,adjust=False,ignore_na=False).mean()
        
        stock_df = stock_df.dropna()

        #stock_df.drop(['close', 'high', 'low', 'open','volume','sma','vol20'], axis=1, inplace=True)
        stock_df = stock_df.dropna()
        stock_df = stock_df[~(stock_df == np.inf).any(axis=1)]
        if len(stock_df) != 0:
            stock_df.to_csv('{}{}.csv'.format(directory,symbol[:4]))
        try:
            lock.acquire()
            #clear memory
            gc.collect()
        finally:
            lock.release()
        
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)
        
if __name__ == '__main__':

    #start timer
    start = timeit.default_timer()

    symbolslist = open('symbols.txt').read().split('\n')

    directory = './data_nasdaq/'

    threadlist = []

    for u in symbolslist:

        t = threading.Thread(target = symbol_downloader,args=(u,directory))
        t.start()
        threadlist.append(t)
        #sets top limit of active threads to 20
        while threading.activeCount()>20:
            a=0
        #print threading.activeCount()

    for b in threadlist:
        b.join()
    print ("# of threads: ", len(threadlist))

#Uncle's pattern
    import traceback
    gc.collect()
    to_send = ''
    to_save = '^DJI\n'
    directory = './data_nasdaq/'
    # get list with filenames in folder and throw away all non ncsv
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for f in files:
        try:
            ticker_df = pd.read_csv(os.path.join(directory, f))
            jones_df = pd.read_csv(os.path.join(directory,'^DJI.csv'))

            stock_df = pd.merge(ticker_df, jones_df, left_index=True, right_index=True)

            #Today's closing price and 13 day moving average
            closeTdayAct =  float(stock_df['close_x'].iloc[-1])
            sma13Act = float(stock_df['sma13_x'].iloc[-1])

            for i in range(3): #engulfing candel pattern looping through all available data 

                window = 59 #number of days back from today to look at for slope

                i += 1 #start range from 1 instead of 0

                openTday =  float(stock_df['open_x'].iloc[-i])
                closeTday =  float(stock_df['close_x'].iloc[-i])
                openYday = float(stock_df['open_x'].iloc[-(i+1)])
                closeYday = float(stock_df['close_x'].iloc[-(i+1)])

                sma5 = float(stock_df['sma5_x'].iloc[-i])
                sma8 = float(stock_df['sma8_x'].iloc[-i])
                sma13 = float(stock_df['sma13_x'].iloc[-i])
                sma21 = float(stock_df['sma21_x'].iloc[-i])

                sma5_1 = float(stock_df['sma5_x'].iloc[-(i+1)])
                sma8_1 = float(stock_df['sma8_x'].iloc[-(i+1)])
                sma13_1 = float(stock_df['sma13_x'].iloc[-(i+1)])
                sma21_1 = float(stock_df['sma21_x'].iloc[-(i+1)])

                sma5_2 = float(stock_df['sma5_x'].iloc[-(i+2)])
                sma8_2 = float(stock_df['sma8_x'].iloc[-(i+2)])
                sma13_2 = float(stock_df['sma13_x'].iloc[-(i+2)])
                sma21_2 = float(stock_df['sma21_x'].iloc[-(i+2)])  

                sma34 = float(stock_df['sma34_x'].iloc[-i])
                sma55 = float(stock_df['sma55_x'].iloc[-i])
                sma89 = float(stock_df['sma89_x'].iloc[-i])
                sma144 = float(stock_df['sma144_x'].iloc[-i])
                sma233 = float(stock_df['sma233_x'].iloc[-i])

                #MACD values
                MACD = float(stock_df['MACD_x'].iloc[-i])
                MACD_signal = float(stock_df['MACD_signal_x'].iloc[-i])
                MACD_1 = float(stock_df['MACD_x'].iloc[-(i+1)])
                MACD_signal_1 = float(stock_df['MACD_signal_x'].iloc[-(i+1)])

                s5 =  ((sma5-sma5_1)/sma5)*1000
                s8 =  ((sma8-sma8_1)/sma8)*1000
                s13 =  ((sma13-sma13_1)/sma13)*1000
                s21 =  ((sma21-sma21_1)/sma21)*1000

                z5 =  ((sma5_1-sma5_2)/sma5_1)*1000
                z8 =  ((sma8_1-sma8_2)/sma8_1)*1000
                z13 =  ((sma13_1-sma13_2)/sma13_1)*1000
                z21 =  ((sma21_1-sma21_2)/sma21_1)*1000

                mktVlcty = float(stock_df['volume_x'].iloc[-i])*float(stock_df['close_x'].iloc[-i])

                volume = (float(stock_df['volume_x'].iloc[-i])+0.001)/(float(stock_df['vol20_x'].iloc[-i])+0.001)

                j_open0 =  float(stock_df['open_y'].iloc[-i])
                j_close0 =  float(stock_df['close_y'].iloc[-i])

                stockPChange = (closeTday-openTday)/openTday 
                jonesPChange = (j_close0-j_open0)/j_open0

                #core buy-in logic
                if  closeTdayAct > sma13Act\
                    and stockPChange > abs(jonesPChange)*3\
                    and openTday < closeTday \
                    and s5 >= z5 and s8 >= z8 and s13 >= z13 and s21 >= z21\
                    and closeTday > sma89 \
                    and sma34 < sma144 and sma55 < sma144 and sma89<sma144\
                    and sma144 < sma233\
                    and closeTday <= 10 and closeTday >= 0.5 \
                    and mktVlcty > 1000000\
                    and volume >= 2:

                    to_save += '{}\n'.format(f[:-4])
                    to_send += '{} has uncle"s pattern with high volume on {}, and is under $10 \n'.format(f[:-4],(stock_df['Unnamed: 0_x'].iloc[-i]))

                #MACD logic
                if  closeTdayAct > sma13Act\
                    and openTday < closeTday \
                    and MACD_1 < MACD_signal_1 and MACD > MACD_signal \
                    and sma34 < sma144 and sma55 < sma144 and sma89<sma144\
                    and sma144 < sma233\
                    and closeTday <= 10 and closeTday >= 0.5\
                    and mktVlcty > 1000000\
                    and volume >= 2:

                    to_save += '{}\n'.format(f[:-4])
                    to_send += '{} has MACD signal with high volume on {}, and is under $10 \n'.format(f[:-4],(stock_df['Unnamed: 0_x'].iloc[-i]))                


        except IndexError:
            print("{} has too few rows".format(f))
            pass
        except Exception:
            print(f,stock_df['Unnamed: 0_x'].iloc[-i])
            print(traceback.format_exc())
            # or
            print(sys.exc_info()[2])
            pass
#replace intraday symbols file
    save_list = to_save.split('\n')

    save_list = list(set(save_list)) #remove duplicates

    reg=re.compile('^[a-zA-Z]+$') #only take symbols with alphabets 
    save_list = [s for s in save_list if reg.match(s)]

    save_list.append('^DJI')

    to_save = ''
    for i in save_list:
        to_save += '{}\n'.format(i)
    with open('C:\\Users\\Richard\\Desktop\\Python\\Investing2.0\\Intraday_Analysis\\symbolsIntraD.txt', 'w+') as symbol_file:
        symbol_file.write(to_save)
        
#email output
    with open('C:\\Users\\Richard\\Desktop\\Python\\hotmail.txt', 'rb') as f:
        email_list = str(f.read()).split(',')
        emailAddress = email_list[0][2:]
        password = email_list[1][:-1]

        msg = MIMEText(to_send)
        recipients = [emailAddress, 'michelleusdenski@gmail.com','Guowei88888@msn.com']#'jacob.si@outlook.com','Greggh_101@hotmail.com','Guowei88888@msn.com']
        msg['Subject'] = '%s stock analysis: Daily Candle potentials - %s' % (str(datetime.datetime.today().strftime('%Y-%m-%d')), str(len(to_send.split('\n'))-1))
        msg['From'] = emailAddress
        msg['To'] = ', '.join(recipients)
        try:
            s = smtplib.SMTP('smtp-mail.outlook.com', 25)
            s.ehlo()  # Hostname to send for this command defaults to the fully qualified domain name of the local host.
            s.starttls()  # Puts connection to SMTP server in TLS mode
            s.ehlo()
            s.login(emailAddress, password)
            s.sendmail(emailAddress, recipients, msg.as_string())
            s.quit()
            print ('email sent to: %s' % emailAddress)
        except:
            raise