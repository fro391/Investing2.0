import threading
import sys, httplib, urllib
from time import gmtime, strftime
import timeit
from chunks import chunks

try: import simplejson as json
except ImportError: import json

global lock
lock = threading.Lock()

PUBLIC_API_URL = 'http://query.yahooapis.com/v1/public/yql'
DATATABLES_URL = 'store://datatables.org/alltableswithkeys'
HISTORICAL_URL = 'http://ichart.finance.yahoo.com/table.csv?s='
RSS_URL = 'http://finance.yahoo.com/rss/headline?s='
FINANCE_TABLES = {'quotes': 'yahoo.finance.quotes',
                 'options': 'yahoo.finance.options',
                 'quoteslist': 'yahoo.finance.quoteslist',
                 'sectors': 'yahoo.finance.sectors',
                 'industry': 'yahoo.finance.industry'}

def executeYQLQuery(yql):
	conn = httplib.HTTPConnection('query.yahooapis.com')
	queryString = urllib.urlencode({'q': yql, 'format': 'json', 'env': DATATABLES_URL})
	conn.request('GET', PUBLIC_API_URL + '?' + queryString)
	return json.loads(conn.getresponse().read())

def __format_symbol_list(symbolList):
	return ",".join(["\""+stock+"\"" for stock in symbolList])


class QueryError(Exception):

	def __init__(self, value):
		self.value = value

	def __str__(self):
		return repr(self.value)

def __is_valid_response(response, field):
	return 'query' in response and 'results' in response['query'] \
		and field in response['query']['results']

def __validate_response(response, tagToCheck):
	if __is_valid_response(response, tagToCheck):
		quoteInfo = response['query']['results'][tagToCheck]
	else:
		if 'error' in response:
			raise QueryError('YQL query failed with error: "%s".'
				% response['error']['description'])
		else:
			raise QueryError('YQL response malformed.')
	return quoteInfo

def get_current_info(symbolList, columnsToRetrieve='*'):
    """Retrieves the latest data (15 minute delay) for the
    provided symbols."""
    try:
        columns = ','.join(columnsToRetrieve)
        symbols = __format_symbol_list(symbolList)

        yql = 'select %s from %s where symbol in (%s)' \
              %(columns, FINANCE_TABLES['quotes'], symbols)
        response = executeYQLQuery(yql)
        V = __validate_response(response, 'quote')
        #each V has n number of responses
        for item in V:
            #only working with items with a name
            if item['Name']is not None:
                #preparing variable to be written to file
                ToWrite = (str(item['symbol'])+","+ str(item['LastTradeDate']).replace(',','') +","+ str(item['LastTradePriceOnly'])+","+ str(item['PercentChange'])+","+ str(item['Volume'])+","+ str(item['AverageDailyVolume'])+","+ str(item['ChangeFromFiftydayMovingAverage'])+","+ str(item['ChangeFromTwoHundreddayMovingAverage'])+","+ str(item['MarketCapitalization'])+","+ str(item['EarningsShare'])+","+ str(item['PriceSales'])+","+ str(item['YearHigh'])+","+ str(item['YearLow'])+'\n')
                lock.acquire()
                try:
                    myfile.write(str(ToWrite))
                finally:
                    lock.release()
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print (message)

start = timeit.default_timer()

#creating file in local 'data' directory
with open('data\keystats'+strftime("%Y-%m-%d", gmtime())+'.csv', 'w+') as myfile:
    myfile.write('Ticker,LastTradeDate,LastTradePriceOnly,PercentChange,Volume,AvgVol,50sma%,200sma%,MktCap,EPS,PriceSales,YearHigh,YearLow'+'\n')

with open("symbols_nasdaq.txt") as symbolfile:
    symbolslistR = symbolfile.read()
    symbolslist = symbolslistR.split('\n')

#breaks down symbols list into n chunks
symbolChunks = list(chunks(symbolslist,20))

threadlist = []

#open "myfile" file for SentimentRSS to write in
with open('data\keystats'+strftime("%Y-%m-%d", gmtime())+'.csv', 'a') as myfile:

    for u in symbolChunks:
        t = threading.Thread(target = get_current_info,args=(u,))
        t.start()
        threadlist.append(t)
        #sets top limit of active threads to 50
        while threading.activeCount()>50:
            a=0
        #print threading.activeCount()
    #finishes threads before closing file
    for b in threadlist:
        b.join()

stop = timeit.default_timer()
print ("start= ",start,"stop= ",stop)