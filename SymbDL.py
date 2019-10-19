import urllib2
import json
import datetime
import time
import threading
import timeit

#declare global lock object
global lock
lock = threading.Lock()

def symbol_update (txtF,dirPath):
    '''
    adds new found stock symbols to symbols list
    first run YahooTickerDownloader.py stocks to get new symbols
    '''
    #old symbols list
    with open(txtF,'r') as symbolfile:
        symbolslistR = symbolfile.read()
        symbolslist = symbolslistR.split('\n')
    len1 = len(symbolslist)

    scrappedSymbol = []

    #extracting north american stocks
    with open(dirPath) as data_file:
        scrapefile = json.load(data_file)
    for i in scrapefile:
        if '.' not in i['Ticker'] and i['Exchange']!= 'OBB'and i['Exchange']!= 'PNK'and i['Exchange']!= 'TOR'and i['Exchange']!= 'BTS'and i['Exchange']!= 'VAN':
            scrappedSymbol.append(str(i['Ticker']))
    #Canadian stocks
    for j in scrapefile:
        if str(j['Exchange']) == 'TOR' and str(j['Exchange']) == 'VAN':
            scrappedSymbol.append(str(j['Ticker']))

    #adding new symbols to old list while removing duplicates
    for t in scrappedSymbol:
        if t not in symbolslist:
            symbolslist.append(t)
    len2 = len(symbolslist)

    #write new list to file
    with open (txtF,'w+') as symbolfile:
        #counter for not adding '\n' at the last line
        counter = 0
        for s in symbolslist:
            counter += 1
            if counter < len(symbolslist):
                symbolfile.write(str(s)+ '\n')
            else:
                symbolfile.write(str(s))

    print "Added", len2-len1, "stocks"

def symbol_screen (symbol,txtF):
    '''
    take out from symbol list inactive stocks
    :param symbol: text
    :return: null
    '''
    dt = datetime.datetime.now()
    UnixTime = int(time.mktime(dt.timetuple()))
    #https://finance-yql.media.yahoo.com/v7/finance/chart/KING?period2=1430672173&period1=1378832173&interval=1d&indicators=quote%7Cbollinger~20-2%7Csma~50%7Csma~50%7Csma~50%7Cmfi~14%7Cmacd~26-12-9%7Crsi~14%7Cstoch~14-1-3&includeTimestamps=true&includePrePost=false&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com
    url='https://finance-yql.media.yahoo.com/v7/finance/chart/'+symbol+'?period2='+str(UnixTime)+'&period1='+str(UnixTime-86400*4)+'&interval=1d&indicators=quote%7Cbollinger~20-2%7Csma~60%7Csma~20%7Csma~5%7Cmfi~14%7Cmacd~26-12-9%7Crsi~14%7Cstoch~14-1-3&includeTimestamps=true&includePrePost=false&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com'
    #use legitimate header so bot won't pick up
    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
       'Connection': 'keep-alive'}

    try:
        request = urllib2.Request(url,headers = hdr)
        htmltext = urllib2.urlopen(request)
    except:
        remove(symbol,txtF)
        return None

    try:
        #dictionaries
        data = json.load(htmltext)
        if len(data['chart']['result'][0]) <=2:
            remove (symbol,txtF)
            return None
        else:
            LASTTRADEDATE = data['chart']['result'][0]['timestamp']

            #last trade date and last closing price
            lastTradeDate = LASTTRADEDATE [-1]

            if lastTradeDate <= UnixTime - 604800: #if last trade date is more than 7 days ago
                remove(symbol,txtF)
            else:
                return None

    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print message, symbol

def remove (symbol,txtF):
    #remove symbol from symbol list file if it's no longer trading
    lock.acquire()
    try:
        with open (txtF, 'r') as symblFile: #remove symbol from old list
            symbR = symblFile.read()
            symbL = symbR.split('\n')
            symbL.remove(symbol)

        with open (txtF,'w+') as symbolfile: #write new list to file
            #counter for not adding '\n' at the last line
            counter = 0
            for s in symbL:
                counter += 1
                if counter < len(symbL):
                    symbolfile.write(str(s)+ '\n')
                else:
                    symbolfile.write(str(s))
    finally:
        print symbol + ' removed'
        lock.release()

if __name__ == '__main__':
    symbFile = 'symbols.txt'
    drctry = 'C:\Users\Richard\stocks.json'
    symbol_update(symbFile,drctry)

    threadlist = []
    with open (symbFile,'r') as thrd_ls:
        symbLs = thrd_ls.read().split('\n')

        for u in symbLs:

            t = threading.Thread(target = symbol_screen,args=(u,symbFile,))
            t.start()
            threadlist.append(t)
            #sets top limit of active threads to 50
            while threading.activeCount()>50:
                a=0
            #print threading.activeCount()

        for b in threadlist:
            b.join()
        print "# of threads: ", len(threadlist)
