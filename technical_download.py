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

#declare global lock object
global lock
lock = threading.Lock()


def symbol_downloader(symbol, directory, days=10000, days_ago=0):
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    # web variables
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + symbol + '?period1=' + str(
        UnixTime - 86400 * (days + days_ago)) + '&period2=' + str(
        UnixTime - 86400 * days_ago) + '&interval=1d&indicators=quote%7Csma~50&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-CA&region=CA&corsDomain=ca.finance.yahoo.com'
    # proxies
    http_proxy = ''
    https_proxy = ''
    ftp_proxy = ''
    proxyDict = {
        "http": http_proxy,
        "https": https_proxy,
        "ftp": ftp_proxy
    }

    hdr = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive'}

    try:
        resp = requests.get(url, headers=hdr)
        if resp.status_code != 200:
            # This means something went wrong.
            raise ApiError('GET /tasks/ {}'.format(resp.status_code))
        data = json.loads(resp.text)
        # json unpack
        timestamp = data['chart']['result'][0]['timestamp']
        timestamp = [datetime.datetime.fromtimestamp(x).strftime('%Y%m%d') for x in timestamp]
        quote = data['chart']['result'][0]['indicators']['quote'][0]
        sma50 = data['chart']['result'][0]['indicators']['sma'][0]['sma']
        stock_df = pd.DataFrame(quote)
        # index is symbol and timestamp
        stock_df.index = [str(x) for x in timestamp]

        # moving averages
        stock_df['sma'] = sma50
        stock_df['vol20'] = stock_df['volume'].rolling(window=20).mean()
        # predict on yesterday's averages
        stock_df['sma'] = stock_df['sma'].shift(1)
        stock_df['vol20'] = stock_df['vol20'].shift(1)
        stock_df = stock_df.dropna()

        # derived columns
        stock_df['close'] = stock_df['close'] / stock_df['sma']
        stock_df['high'] = stock_df['high'] / stock_df['sma']
        stock_df['low'] = stock_df['low'] / stock_df['sma']
        stock_df['open'] = stock_df['open'] / stock_df['sma']
        stock_df['volume'] = stock_df['volume'] / stock_df['vol20']

        # stock_df.drop(['close', 'high', 'low', 'open','volume','sma','vol20'], axis=1, inplace=True)
        stock_df = stock_df.dropna()
        stock_df = stock_df[~(stock_df == np.inf).any(axis=1)]
        if len(stock_df) != 0:
            stock_df.to_csv('{}{}.csv'.format(directory, symbol[:4]))

    except Exception as ex:
        pass

if __name__ == '__main__':

    #start timer
    start = timeit.default_timer()

    symbolslist = open('symbols_nasdaq.txt').read().split('\n')

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

    #Analyze
    to_send = ''
    # get list with filenames in folder and throw away all non ncsv
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for f in files:
        try:
            stock_df = pd.read_csv(os.path.join(directory, f))
            if float(stock_df.tail(1)['close']) >= 1.2 and float(stock_df.tail(1)['volume']) >= 1.5 and float(stock_df.tail(1)['sma']) <= 20 and float(stock_df.tail(1)['sma']) >= 2:
                to_send += '{} has high price and volume movement on {}, and is under $20 \n'.format(f[:-4],(stock_df['Unnamed: 0'].iloc[-1]))

        except:
            pass

    print (to_send)

    # email output
    with open('C:\\Users\\Richard\\Desktop\\Python\\hotmail.txt', 'rb') as f:
        email_list = str(f.read()).split(',')
        emailAddress = email_list[0][2:]
        password = email_list[1][:-1]

        msg = MIMEText(to_send)
        recipients = [emailAddress, 'michelleusdenski@gmail.com', ]  # 'jacob.si@outlook.com','Greggh_101@hotmail.com']
        msg['Subject'] = '%s stock analysis: potentials - %s' % (
        str(datetime.datetime.today().strftime('%Y-%m-%d')), str(len(to_send.split('\n') - 1)))
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

    # timer
    stop = timeit.default_timer()
    print ("seconds of operation: ", stop - start)