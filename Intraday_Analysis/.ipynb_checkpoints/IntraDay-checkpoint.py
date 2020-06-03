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

import traceback

#declare global lock object
global lock
lock = threading.Lock()

def symbol_downloader_intraday (symbol, directory, days=30, days_ago=0):
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    #web variables
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/'+symbol+'?period1='+str(UnixTime-86400*(days+days_ago))+'&period2='+str(UnixTime-86400*days_ago)+'&interval=5m&indicators=quote%7Csma~60&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-CA&region=CA&corsDomain=ca.finance.yahoo.com'
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
        timestamp = [datetime.datetime.fromtimestamp(x).strftime('%Y%m%d-%H%M') for x in timestamp]
        quote = data['chart']['result'][0]['indicators']['quote'][0]
        stock_df = pd.DataFrame(quote)
        #index is symbol and timestamp
        stock_df.index = [str(x) for x in timestamp]

        #moving averages
        stock_df['vol20'] = stock_df['volume'].rolling(window=20).mean()
        stock_df['sma5'] = stock_df['close'].rolling(window=5).mean()
        stock_df['sma8'] = stock_df['close'].rolling(window=8).mean()
        stock_df['sma13'] = stock_df['close'].rolling(window=13).mean()
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
        
    except Exception:
        print(symbol)
        print(traceback.format_exc())
        # or
        print(sys.exc_info()[2])
        
if __name__ == '__main__':
    
    #Delete files in Directory
    directory = './data5m/'
    filelist = [ f for f in os.listdir(directory) if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join(directory, f))

    #start timer
    start = timeit.default_timer()

    symbolslist = open('symbolsIntraD.txt').read().split('\n')

    directory = './data5m/'

    threadlist = []

    for u in symbolslist:

        t = threading.Thread(target = symbol_downloader_intraday,args=(u,directory))
        t.start()
        threadlist.append(t)
        #sets top limit of active threads to 20
        while threading.activeCount()>20:
            a=0
        #print threading.activeCount()

    for b in threadlist:
        b.join()
    print ("# of threads: ", len(threadlist))

#IntraDay Buy-In signal @5m candle sticks
    gc.collect()
    to_send = ''
    directory = './data5m/'
    # get list with filenames in folder and throw away all non ncsv
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for f in files:
        try:
            ticker_df = pd.read_csv(os.path.join(directory, f))
            jones_df = pd.read_csv(os.path.join(directory,'^DJI.csv'))

            stock_df = pd.merge(ticker_df, jones_df, left_index=True, right_index=True)

            for i in range(int((len(stock_df)*0.3))): #engulfing candel pattern looping through all available data 

                window = 59 #number of days back from today to look at for slope

                i += 1 #start range from 1 instead of 0

                open0 =  float(stock_df['open_x'].iloc[-i])
                close0 =  float(stock_df['close_x'].iloc[-i])
                open_1 = float(stock_df['open_x'].iloc[-(i+1)])
                close_1 = float(stock_df['close_x'].iloc[-(i+1)])

                sma5 = float(stock_df['sma5_x'].iloc[-i])
                sma8 = float(stock_df['sma8_x'].iloc[-i])
                sma13 = float(stock_df['sma13_x'].iloc[-i])

                mktVlcty0 = float(stock_df['volume_x'].iloc[-i])*float(stock_df['close_x'].iloc[-i])

                volume0 = (float(stock_df['volume_x'].iloc[-i])+0.001)/(float(stock_df['vol20_x'].iloc[-i])+0.001)

                j_open0 =  float(stock_df['open_y'].iloc[-i])
                j_close0 =  float(stock_df['close_y'].iloc[-i])
                stockPChange = (close0-open0)/open0 
                jonesPChange = (j_close0-j_open0)/j_open0

                timeOfDay = stock_df['Unnamed: 0_x'].iloc[-i][-4:]
                date0 = stock_df['Unnamed: 0_x'].iloc[-i][:8]
                tday_date = str(datetime.datetime.today().strftime('%Y%m%d'))

                #core logic
                if  stockPChange > abs(jonesPChange)*10\
                    and sma5<close0 \
                    and sma5> open0 \
                    and sma5 > sma8 and sma8 > sma13 \
                    and close0 <= 20 and close0 >= 0.5 \
                    and mktVlcty0 > 100000\
                    and volume0 >= 2\
                    and timeOfDay != '0930'\
                    and date0 == tday_date:

                    to_send += '{} has 5m buy-in signal with high volume on {}, and is under $20. Close price: {} \n'.format(f[:-4],(stock_df['Unnamed: 0_x'].iloc[-i]),close0)

        except IndexError:
            print("{} has too few rows".format(f))
            pass
        except Exception:
            print(traceback.format_exc())
            # or
            print(sys.exc_info()[2])
            pass

#Send email if there are buy-in signals
    if len(to_send) > 0:
        #email output
        with open('C:\\Users\\Richard\\Desktop\\Python\\hotmail.txt', 'rb') as f:
            email_list = str(f.read()).split(',')
            emailAddress = email_list[0][2:]
            password = email_list[1][:-1]

            msg = MIMEText(to_send)
            recipients = [emailAddress, 'michelleusdenski@gmail.com','guowei88888@msn.com']#'jacob.si@outlook.com','Greggh_101@hotmail.com']
            msg['Subject'] = '%s stock analysis - Intraday Buy-ins: potentials - %s' % (str(datetime.datetime.today().strftime('%Y%m%d-%H%M')), str(len(to_send.split('\n'))-1))
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
    else: 
        print('No buy-in signals')