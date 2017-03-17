# coding: utf-8

"""

WallstreetDB.py: Wallstreet Data Acquisition
©2017 David Williams. Creative Commons License applies. 
You may use this software freely in your work by clearly and conspicuously acknowledging its author in the copyright AND 
the documentation of your software with the name of the author as well as this link to the original software on github: .

See readme file for usage details. 

"""


# Imports

import urllib, sys, os, time, csv, re, os, requests, sqlite3, holidays, time, datetime
from bs4 import BeautifulSoup as bsoup
import numpy as np
import pandas as pd
import pandas.io.sql as pdsql
from yahoo_finance import Share as yf


"""
Get All Stock Symbols From Exchanges. Data Sources and Variables

"""

# NASDAQ URL chain for collecting all current exchange data in CSV format
all_cur_stocks_csv_base_url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange='
all_cur_stocks_csv_exchange = ['nasdaq', 'nyse']
all_cur_stocks_csv_tail = '&render=download'

# Morningstar chain for recreating 10k/10q financials for 5yr/5qtr on any stock in CSV format.
mngstar_fin_csv_base_url = 'http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t='
mngstar_fin_csv_exchange = ['XNAS:','XNYS:']
mngstar_fin_csv_report_region = '&region=usa&culture=en-US&cur=&reportType='
mngstar_fin_csv_report_type = ['is','bs','cf']
mngstar_fin_csv_report_period = '&period=' 
mngstar_fin_csv_report_freq_str = ['3','12']
mngstar_fin_csv_tail = '&dataType=A&order=asc&columnYear=5&curYearPart=1st5year&rounding=3&view=raw&denominatorView=raw&number=3'

# Morningstar URL chain for 10yr pricing CSV 
stock_price_mngstar_csv_base_url = 'http://performance.morningstar.com/perform/Performance/stock/exportStockPrice.action?t='
stock_price_mngstar_csv_exchange = ['XNAS:', 'XNYS:']
stock_price_mngstar_csv_period = '&pd=10y' # this can be adjusted to 5D, YTD, 5y, etc as desired
stock_price_mngstar_csv_freq_str= '&freq='
stock_price_mngstar_csv_freq_period = ['d','w','m','a'] # can adjust freq=period
stock_price_mngstar_csv_tail = '&sd=&ed=&pg=0&culture=en-US&cur=USD'

# Morningstar URL chain for individual stock dividend tables for the past n years.
stock_div_table_mngstar_head = 'http://performance.morningstar.com/perform/Performance/stock/'
stock_div_table_mngstar_type = ['upcoming-dividends','dividend-history']
stock_div_table_mngstar_action = '.action?&t='
stock_div_table_mngstar_exchange = ['XNAS:','XNYS:']
stock_div_table_mngstar_region = '&region=usa&culture=en-US&cur=&ops=clear&ndec=2'
stock_div_table_mngstar_tail = '&y=' # don't forget to specify the number of years of histrical data as 5, 10, 20, etc

# Morningstar URL chain for key financial data for 10-yr period
stock_financials_mngstar_head = 'http://financials.morningstar.com/finan/ajax/exportKR2CSV.html?&callback=?&t='
stock_financials_mngstar_exchange = ['XNAS:','XNYS:']
stock_financials_mngstar_tail = '&region=usa&culture=en-US&cur=&order=asc'



"""
DB Setup & Management Methods. Note: all table creation is handled by PANDAS. 
"""

# default database
sqlite_db = './mydb.db'
cnx = sqlite3.connect(sqlite_db)
cur = cnx.cursor()
# def setDBPath(db_path):
# 	"""

# 	Set a database path for the stock data you're about to collect. 

# 	This function takes one string, an absolute path to your desired database location
# 	with the format /[folder]/[sub-folder]/[yourfile].db
# 	Note that there is no path checking built into this function. Make sure your path 
# 	works before calling the function. 

# 	Returns a successful message with your path name if successful. Otherwise, 
# 	returns a tuple of (false, error message).

# 	Example Usage: setDBPath('./database/mystockdb.db')

# 	"""
# 	try: 
# 		sqlite_db = db_path
# 		cnx = sqlite3.connect(sqlite_db)
# 		cur = cnx.cursor()
# 		return 'Successfully set DB path to {path}.'.format(path=sqlite_db)

# 	except Exception as e:
# 		return False, e


def dropAllTables():
	"""
	
	Danger! This function drops all tables in your currently active DB. 

	Use this function to erase the information in your current DB and start over. 
	Probably a good idea to remove this function for your production code.

	Returns True if successfully remove all tables. Otherwise, returns a tuple
	of (False, error message).

	Example Usage: 

	"""
	try:
		all_db_tbls = cur.execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;').fetchall()        
		for i in all_db_tbls:
			cur.execute('DROP TABLE IF EXISTS {tbl}'.format(tbl = i[0]))
		return True

	except Exception as e:
		return (False, e)


def symbolTableExists():
	"""

	Tells whether or not the Symbol table exists. If not, it throws an error before any functions can try to 
	add data to the database. 

	Return value of True if a table exists, otherwise False. If error, returns tuple (False, error message)

	Example Usage: 

	"""

	try:
		if cur.execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="all_stocks_key";').fetchone() is not None:
			return True
		return False 
	except Exception as e:
		return False, e


def createSymbolsKeyTable(symbols): 
	"""

	Initializes the xchanges DB by creating all needed tables for all stocks listed in NASDAQ and NYSE.

	This function receives a PANDAS dataframe with fields stock symbol and exchange.
	If the table is added to the DB correctly, returns True. If table already exists, 
	a ValueError is returned with False in a tuple. 

	Example Usage: createSymbolsKeyTable(current_stocks[['Symbol', 'Market']])

	"""

	try:
        # consider adding your own logic to query the DB and check if there are any new stocks in the NASDAQ's listings 
        # and update by appending a new frame to the existing DB.
		if symbolTableExists() == True:
			raise ValueError("The Symbols key table already exists. Move along.")
		symbols.to_sql('all_stocks_key', con=cnx, if_exists='replace', index=False)
		return True
	except Exception as e:
		return (False, e)


def makeStockListURL(exchange):
	"""
	Creates a URL for PANDAS to download a CSV list of all current stocks from NASDAQ.com

	Function receives a string of either 'NYSE' or 'NASDAQ' for the exchange, which it uses
	to combine the URL path to the csv file.

	Returns the complete URL as a string.

	Example Usage: 

	"""

	the_exchange = all_cur_stocks_csv_exchange[0]
	if exchange == 'NYSE': 
		the_exchange = all_cur_stocks_csv_exchange[1]

	return all_cur_stocks_csv_base_url + the_exchange + all_cur_stocks_csv_tail


def getAllCurStocks(exchanges):
	"""

	Convenience function for donwloading and cleaning the csv file of all stocks from NASDAQ and NYSE.

	The function takes either a list or tuple of len = 2, consisting of the strings 'NYSE' and 'NASDAQ'.
	It calls the function that makes the URL for downloading the data, and then retreives the data.
	It also performs several cleanup functinos on the data, converting numerical strings to floats, and
	putting data in a more manageable format.

	Returns a single dataframe of all stocks traded on the exchanges requested.

	An example of using this function is as follows: 

		exchanges = ['NASDAQ', 'NYSE']
		current_stocks = getAllCurStocks(exchanges)
		createSymbolsKeyTable(current_stocks[['Symbol', 'Market']]) # uses only the Symbol and Market field names in the returned dataframe to create the table

	Finally, you can test to make sure tables were created with the following:

		all_db_tables = cur.execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;').fetchall() # 'SELECT * FROM all_stocks_key')
		print(len(all_db_tables), all_db_tables)

	Example Usage: 

	"""
    
	stock_lists_to_download = [makeStockListURL(exchanges[0]), makeStockListURL(exchanges[1])] 
	exchange_data = [pd.read_csv(i, index_col = 0, encoding='utf-8') for i in stock_lists_to_download]
    # make column in each frame for Exchange and assign the market that the stock belongs to
	for idx, i in enumerate(exchange_data): 
		i.loc[:,'Market'] = 'NASDAQ' if idx == 0 else 'NYSE' 
    # merge data into single frame
	all_exchanges = pd.concat([exchange_data[0], exchange_data[1]])
    # drop the Unnamed and Summary Quote columns
	all_exchanges.drop(['Unnamed: 8', 'Summary Quote'], axis=1, inplace=True)
    # drop all n/a(s) in the LastSale column b/c I don't care about stock that's not selling. Research more ltr.
	all_exchanges = all_exchanges[ (all_exchanges.loc[:,'LastSale'] != 'n/a') & (all_exchanges.loc[:, 'LastSale'] != None) ]
    # cast all numeric values in LastSale as float instead of string
	all_exchanges.loc[:, 'LastSale'] = all_exchanges.loc[:,'LastSale'].astype(float)
    # add column for marketcap symbol and remove all symbols and numbers from marketcap that to get the multiplier 
	all_exchanges['MarketCapSym'] =  all_exchanges['MarketCap'].replace('[$0-9.]', '', regex=True)   
    # remove $ and letters from MarketCap fields
	all_exchanges['MarketCap'] = all_exchanges['MarketCap'].apply(lambda x: re.sub('[$MB]', '', x))
	all_exchanges.reset_index(inplace=True)
    # remove any unwanted whitespace from symbol or name
	all_exchanges['Symbol'] = all_exchanges['Symbol'].apply(lambda x: re.sub('\s+', '', x))
    # replace all n/a values in MarketCap with np.NAN
	all_exchanges[all_exchanges['MarketCap'] == 'n/a'] = np.NAN
    # convert MarketCap to a float. Find out if chaining this way is bad.
	all_exchanges['MarketCap'] = all_exchanges['MarketCap'].astype(float)
    # round the LastSale column
    # all_exchanges.MarketCap = all_exchanges.MarketCap.round(2)
	all_exchanges['LastSale'] = all_exchanges['LastSale'].round(2)
    # rename industry column
	all_exchanges.rename(columns={'industry':'Industry'}, inplace=True)
	all_exchanges = all_exchanges[all_exchanges['Symbol'].notnull()] 
    # remove any duplicate stock symbols using pandas unique() method
	all_exchanges.drop_duplicates(subset='Symbol', keep='first', inplace=True)
    
	return all_exchanges


def get10YrPriceHistory(symbol):
	"""

	Get 10Y price history, one symbol at a time.

	Function takes a single stock symbol in the form of a string. That symbol is used to build a 
	URL path to collect the 10Y price history as a CSV. The data is loaded into a PANDAS dataframe.

	Returns a pandas dataframe if successful. Otherwise, returns a tuble of (False, error message).

	Example usage: get10YrPriceHistory(('ULTI', 'NASDAQ'))

	"""

	try:
		exchange = stock_price_mngstar_csv_exchange[0] if symbol[1] == 'NASDAQ' else stock_price_mngstar_csv_exchange[1]
		price_history_path = (stock_price_mngstar_csv_base_url + 
								exchange + symbol[0] +
								stock_price_mngstar_csv_period +
								stock_price_mngstar_csv_freq_str + 
								stock_price_mngstar_csv_freq_period[0] + 
								stock_price_mngstar_csv_tail)    
        
		return pd.read_csv(price_history_path, header=1, encoding = 'utf8') # header is on second row. remove first.
    
	except Exception as e:
		return False, e


def get10KQReport(symbol, report_type, freq):
	"""

	Get 10k/10q reports (Income, Balance, Cashflow) from Morningstar, one symbol at a time. 

	Function requires a tuple consisting of a stock symbol and the exchange ('SYMBOL', 'EXCHANGE'), 
	a report category of type string, and the time frequency of the report data as an integer. 
	Available options for report_type are 'is','bs','cf'. Frequency can be either 3 or 12. 
	If no report_type is specified, function falls back to a cashflow sheet. The default frequency is 
	12 month, which works both for 5yr 10K reports and for TTM 10Q reports.

	Returns the requested report packaged in a PANDAS dataframe.

	Example Usage: get10KQReport(('ANDA', 'NASDAQ'), 'bs', 12)

	"""

	try:
		exchange = mngstar_fin_csv_exchange[0] if symbol[1] == 'NASDAQ' else mngstar_fin_csv_exchange[1]
		frequency = mngstar_fin_csv_report_freq_str[0] if freq == 3 else mngstar_fin_csv_report_freq_str[1]
		# set the report type        
		report = None
		if report_type == 'is': 
			report = mngstar_fin_csv_report_type[0]
		elif report_type == 'bs': 
			report = mngstar_fin_csv_report_type[1]
		else: 
			report = mngstar_fin_csv_report_type[2]
        # create the report URL path
		report_path = (mngstar_fin_csv_base_url + exchange + 
						symbol[0] + mngstar_fin_csv_report_region +
						report + mngstar_fin_csv_report_period + 
						frequency + mngstar_fin_csv_tail)
        
		return pd.read_csv(report_path, header=1, encoding='utf-8') # header is on second row. remove first.
    
	except Exception as e:
		return False, e


def commitFinancialsData(report, report_type, report_period):
	"""

	Handles commitment of 10K/Q reports to the database in their respective tables.

	This function will commit a generated financial report to DB and create the appropriate table if it doesn't exist.
	The required report argument is the dataframe created by get10KQReport(). The stock symbol included is used to 
	check whether the financial history for this stock is present in this report_type's table. Report_type 
	consists of a string with options of 'is', 'bs', and 'cf' for income, balance, and cashflow sheets. Report_period 
	is an integer of either 3 or 12 for 3-month and 12-month.

	Returns True if the commit was successful, otherwise it will return a tuple (False, ValuerError or other exception).

	Note: PANDAS will implicitly set up tables. No need to write separate funcs to set up those tables or specify col names.

	Example Usage: 

	"""

	try:
        # see if the stock symbol is already there and raise an error if so.
		if financialHistoryExists(report.index[0], report_type, report_period) is True: # must specify if true
			raise ValueError('Uh oh. There\'s already a record matching this one. Try using commitIndividualFinancials() method to update the financial info instead.')
            
        # sort by report type
		if report_type == 'is':
            # cols = ['Symbol', 'Income Item', 'Yr 1', 'Yr 2', 'Yr 3', 'Yr 4', 'Yr 5']
			if report_period == 3: 
				report.to_sql('Ten_Q_Income', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
                # clean_df_db_dups()
			elif report_period == 12: # report goes into annuals
				report.to_sql('Ten_K_Income', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
			else: # catch formatting error
				raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))

		elif report_type == 'bs':
			cols = []
			if report_period == 3:
				report.to_sql('Ten_Q_Balance', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
			elif report_period ==12: 
				report.to_sql('Ten_K_Balance', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
			else: 
				raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))
            
		elif report_type == 'cf':
			cols = []
			if report_period == 3:
				report.to_sql('Ten_Q_Cashflow', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
			elif report_period ==12:
				report.to_sql('Ten_K_Cashflow', con=cnx, if_exists='append') # insert values into DB using ?? operators                
				return True
			else: 
				raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))
        
		else: # there was a formatting error in function call. raise exception
			raise ValueError("Formatting error in function call. Check your variables {rep_type} and {rep_period}".format(rep_type=report_type, rep_period=report_period))
    
	except Exception as e:
		return False, e


def financialHistoryExists(symbol, report_type, report_period):
	"""

	Tells whether the DB has a financial report for a given stock symbol. 

	Takes a symbol string ('MMM'), report type string ('is', 'bs', or 'cf'), and report period integer 
	(3 or 12) to check the database for the symbols report. 

	Returns True if the stock and its table is already present. Otherwise, it will 
	return either False if no table exists. The final return option is a tuple 
	(False, Error). If you modifiy the functions that use this routine, make sure that 
	your error checking knows how to distinguish between a single False return and the 
	tuple that's returned if there's an error.

	Example Usage: 

	"""

	try:
        # set the table to search
		if report_type == 'is':
			if report_period == 3:
				table = 'Ten_Q_Income'
			elif report_period == 12:
				table = 'Ten_K_Income'
			else: 
				raise ValueError('Incorrect period of {rpt_pd} requested. Try again.'.format(rpt_pd = report_period)) # wrong period specified. Throw and error.
                
		elif report_type == 'bs':
			if report_period == 3:
				table = 'Ten_Q_Balance'
			elif report_period == 12:
				table = 'Ten_K_Balance'
			else:
				raise ValueError('Incorrect period of {rpt_pd} requested. Try again.'.format(rpt_pd = report_period))
                
		elif report_type == 'cf':
			if report_period == 3:
				table = 'Ten_Q_Cashflow'
			elif report_period == 12:
				table = 'Ten_K_Cashflow'
			else: 
				raise ValueError('Incorrect period {rpt_pd} requested. Try again.'.format(rpt_pd = report_period))
            
		else:
			raise ValueError('A report type {rpt} was requested that does not match any you offer. Try again.'.format(rpt = report_type))  # unknown report type..throw an error 
        
        # search the DB for the data
		query = 'SELECT * FROM {tbl} WHERE Symbol = ?'.format(tbl = table)
        
		if cur.execute(query, (symbol,)).fetchone() is not None:
			return True
		else: 
			return (False, 'No records found.')
        
	except Exception as e:
		return (False, e)


def commitPriceHistory(data):
	"""

	commits a stock's price history dataframe to the database. 

	This function receives a dataframe and will check to see if a 10Y price history for the stock it references.
	Note that the history checking routine only looks to see that the referenced stock already exists in the 
	price history table. If so, it will report a ValueError. If you want to do daily updates of stock prices, check 
	the updateStockPrices() function instead.

	Returns a tuple (True, 'Success Message') if successful. Otherwise, returns a tuple (False, error message)

	Example Usage: commitPriceHistory(data)

	"""

	try: 
        # check if the stock symbol is already present
		if priceHistoryExists(data.index[0]) == True:
			raise ValueError("The symbol is already present. Try using updateStockPrice() instead, or delete the existing record.")
        
		data.to_sql('Ten_Yr_Prices', con=cnx, if_exists='append')
		return (True, 'Successfully commited {stock} price history to the DB.'.format(stock=data.index[0]))
    
	except Exception as e:
		return (False, e)


def priceHistoryExists(symbol):
	"""

	Searches the DB's Price History table for the selected symbol and returns True if found.

	This function receives a string 'SYMBOL' for the desired stock lookup. It searches the 
	database's Pricehistory table to find an instance of this symbol. If it does, the function
	returns True. Otherwise, it will return a tuple (False, 'No records msg'). 

	If the function encounters an error , it will also return a tuple (False, error message). 
	Note that any subsequent error checking built into functions that utilize this one will need
	to distinguish between a not-found False and an error False.

	Example Usage: priceHistoryExists('GOOG')

	"""

	try:
        # in future, add double check to make sure this symbol is even in the symbol list...faster lookup probably.
		if cur.execute('SELECT * FROM Ten_Yr_Prices WHERE Symbol = ?', (symbol,)).fetchone() is not None:
			return True
        #otherwise return false
		return (False, 'No records found for {stock}.'.format(stock=symbol))
	except Exception as e:
		return (False, e)


def updateStockPrices(symbol):
	"""

	Updates a single stock's pricing data using the Yahoo API. 

	Not yet finished. The function takes a string 'SYMBOL'.

	The function returns the most recent pricing for a stock at end of each trading day after.
	It first queries the database price history table to find the most recent date. If no price history existss for today's
	date, then a new request will be made using Yahoo's API to retreive the open, high, low, close prices for a stock, 
	as well as the stock's trading volume. This data, plus a timestamp, is appended to the price history table.

	That string must be a valid stock symbol or an error message will result. Relatively simple date and holiday checking 
	is built into the function. For production use, it is recommended to implement the PANDAS holiday and related datetime 
	methods for handling weekdays v weekends, holidays, and any other custom days for your application. 

	The reuturn value is a tuple (True, 'Success message'), or a tuple (False, Error message). 

	Example usage: updateStockPrices('DUK')

	"""

	try:
        # set dates for comparison
		yesterday_query = cur.execute('SELECT Reference FROM Ten_Yr_Prices WHERE Symbol = ? ORDER BY DATE(Reference) DESC LIMIT 1;', (symbol,)).fetchone()
		yesterday = datetime.datetime.strptime(yesterday_query[0], '%Y-%m-%d').date()
		today = datetime.date.today()
        
        # make sure today is not a weekend day
		if today.isoweekday() > 5: #see if the update has already been made today by checking the date
			raise ValueError('{stock} cannot be updated on a weekend. See you next trading day.'.format(stock=symbol))

        # make sure it's not a trading holiday
		elif today in holidays.US():
			raise ValueError('{stock} not updated. Today is a trading holiday. See you next trading day.'.format(stock=symbol))
        
        # make sure today is a new day in the DB.
		elif yesterday >= today: # what if i want to update past stock prices I missed for some reason?            
			raise ValueError('{stock} date already updated today. See you next trading day'.format(stock=symbol)) 
        
        #if all conditions successful, use Yahoo to get the symbol's price data
		price_open = yf(key).get_open()
		price_high = yf(key).get_days_high()
		price_low = yf(key).get_days_low()
		price_close = yf(key).get_price()
		volume = yf(key).get_volume()

		today = time.strftime("%Y-%m-%d")
        
        # send to DB
		cur.execute('INSERT INTO Ten_Yr_Prices VALUES (?,?,?,?,?,?,?)', (symbol, today, price_open, price_high, price_low, price_close, volume))
		cnx.commit()
        
		return True, 'Stock price updated for today!'
        
	except Exception as e:
		return False, e


def createPriceHistoryReport(symbol):
	"""

	Calls get10YrPriceHistory() to package a price history report into a PANDAS dataframe, then cleans and returns the data.

	This function will acquire a price history for the provided symbol, which must be a string and a valid stock symbol, e.g., ('MMM').
	After the data is loaded, the function adds a Symbol field to the price history for tracking in the database, reindexes 
	and renames some fields, properly formats the dates into datetime fields, and converts prices from strings to floats.

	Returns the report as a PANDAS dataframe if successful, otherwise a tuple (False, error message).

	Example Usage: createPriceHistoryReport('MMM')

	"""

	try:
        # get the raw data from morningstar
		price_history = get10YrPriceHistory(symbol)
        # add Symbol column for tracking and adding to DB
		price_history['Symbol'] = symbol[0]
        # reorganize header order
		price_history = price_history.reindex(columns=['Symbol','Date','Open','High','Low','Close','Volume'])
        # rename the Date column for easier processing through SQLite's Date functionality
		price_history.rename(columns={'Date':'Reference'}, inplace=True)
        # convert all dates to ISO formatted yyyy-mm-dd formatted strings
		price_history['Reference'] = price_history['Reference'].apply(lambda x: time.strftime("%Y-%m-%d", time.strptime(x, "%m/%d/%Y")))
        # convert volumes to integers # unicode err on ??? value for some volumes goes to NaN
		price_history['Volume'] = pd.to_numeric(price_history['Volume'].str.replace(',',''), errors='coerce')
        # set index b/f db commit so no duplicate numeric index columns
		price_history.set_index(['Symbol'], inplace=True) 

		return price_history

	except Exception as e:
		return (False, e)


def create10KIncomeReport(symbol):
	"""

	Create a 10K income statement report for a given stock symbol and return data as a PANDAS dataframe.

	This function requires a symbol argument which is a tuple of a ('SYM', 'EXCHANGE'). Allowed values for the 
	first part of the tuple are any valid stock symbol. The Exchange must be either NASDAQ or NYSE. 
	The function uses get10KQReport to generate an income statement packaged in a dataframe. After 
	retreiving the data, it creates a Symbol field for tracking the data inside the TenKIncome database table.
	Some cleaning and shortening of field names also takes place. 

	Returns the income repot as a dataframe if successful. Otherwise, returns a tuple (False, Error message).

	Example Usage: create10KIncomeReport(('ULTI', 'NASDAQ'))

	"""

	try:
		ten_k_income = get10KQReport(symbol, 'is', 12) # note: a slow connection prevents download...will need to test completion
        # add symbol column and reorganize column headers
		ten_k_income['Symbol'] = symbol[0]
        # reindex the columns
		ten_k_income = ten_k_income.reindex(columns=['Symbol', 'Fiscal year ends in December. USD in millions except per share data.', '2011-12', '2012-12', '2013-12', '2014-12', '2015-12', 'TTM'])
        # rename the year columns, oldest first
		ten_k_income.rename(columns={
				'Fiscal year ends in December. USD in millions except per share data.':'Income Item (MM)',
				'2011-12':'Yr 1', # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
				'2012-12':'Yr 2', 
				'2013-12':'Yr 3', 
				'2014-12':'Yr 4', 
				'2015-12':'Yr 5'}, inplace=True)
        #set the index to Symbol column for easy DB insertion
		ten_k_income.set_index(['Symbol'], inplace=True)

		return ten_k_income

	except Exception as e:
		return (False, e)


def create10KBalanceReport(symbol):
	"""

	Create an annual balance report for the last 5 years for any given stock.

	The function uses get10KQReport() to generate the data and then formats the field names 
	to shorten them and become relative date ranges instead of absolute ranges. 
	This function takes a symbol argument that is a tuple of form ('SYMBOL','EXCHANGE'). The symbol is any valid 
	stock symbol. The exchange must be either 'NYSE' or 'NASDAQ'. 

	Return value, if successful, is the balance sheet packaged in a PANDAS dataframe. Otherwise a tuple of 
	(False, error message) is returned.

	Example Usage: create10KBalanceReport(('DDD','NASDAQ'))

	"""

	try:
		ten_k_balance = get10KQReport(symbol, 'bs', 12) # note: a slow connection prevents download...will need to test completion
        # add symbol column and reorganize column headers
		ten_k_balance['Symbol'] = symbol[0]
        # reindex the columns
        # rename the year columns, oldest first
        # rename columns where appropriate
		ten_k_balance.rename(columns={
				'Fiscal year ends in December. USD in millions except per share data.':'Balance Item (MM)',
				'2011-12':'Yr 1', # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
				'2012-12':'Yr 2', 
				'2013-12':'Yr 3', 
				'2014-12':'Yr 4', 
				'2015-12':'Yr 5'}, inplace=True)
        # reindex the columns
		ten_k_balance = ten_k_balance.reindex(columns=['Symbol', 'Balance Item (MM)', 'Yr 1', 'Yr 2', 'Yr 3', 'Yr 4', 'Yr 5'])
        #set the index to Symbol column for easy DB insertion
		ten_k_balance.set_index(['Symbol'], inplace=True)

		return ten_k_balance
        
	except Exception as e:
		return False, e


def create10KCashflowReport(symbol):
	"""

	Create a 10K (annual) Cashflow report.

	Function uses get10KQReport() to generate a cashflow report for the given stock. The downloaded data
	is cleaned and field names are shortened for readability. This function takes a symbol argument that 
	is a tuple of form ('SYMBOL','EXCHANGE'). The symbol is any valid stock symbol. The exchange must be 
	either 'NYSE' or 'NASDAQ'. 

	Return value if successful is the cashflow report pacakged in a PANDAS dataframe. Otherwise will 
	return a tuple (False, error message)

	Example Usage: create10KCashflowReport(('DDD','NASDAQ'))

	"""

	try:
		ten_k_cashflow = get10KQReport(symbol, 'cf', 12) # note: a slow connection prevents download...will need to test completion
        # add symbol column and reorganize column headers
		ten_k_cashflow['Symbol'] = symbol[0]
        # rename columns where appropriate. Don't want to lose denomination. And don't want to lose column years b/c I can just keep adding to get my 10yr for free.
		ten_k_cashflow.rename(columns={'Fiscal year ends in December. USD in millions except per share data.':'Cashflow Item (MM)'}, inplace=True)
        # reindex the columns
		ten_k_cashflow = ten_k_cashflow.reindex(columns=['Symbol', 'Cashflow Item (MM)', '2011-12', '2012-12', '2013-12', '2014-12', '2015-12', 'TTM'])#set the index to Symbol column for easy DB insertion
        # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
        # set the index to Symbol for easy DB insertion
		ten_k_cashflow.set_index(['Symbol'], inplace=True)

		return ten_k_cashflow
        
	except Exception as e:
		return False, e


def create10QIncomeReport(symbol):
	"""

	Create a 10Q (quarterly) income report for a given stock ticker.

	This function uses get10KQReport() to generate a 10Q quarterly report for the given stock. 
	Downloaded field names are shortened for readibility. This function takes a symbol argument that 
	is a tuple of form ('SYMBOL','EXCHANGE'). The symbol is any valid stock symbol. The exchange must be 
	either 'NYSE' or 'NASDAQ'.

	Return value if successful is the quarterly income report packaged in a PANDAS dataframe. Otherwise
	will return a tuple (False, error message). 

	Example Usage: create10QIncomeReport(('MMM','NYSE'))

	"""

	try:
		ten_q_income = get10KQReport(symbol, 'is', 3)
        # add symbol column and reorganize column headers
		ten_q_income['Symbol'] = symbol[0]
        # rename columns where appropriate. Don't want to lose denomination. And don't want to lose column years b/c I can just keep adding to get my 10yr for free.
		ten_q_income.rename(columns={'Fiscal year ends in December. USD in millions except per share data.':'Income Item (MM)'}, inplace=True)
        # reindex the columns
		ten_q_income = ten_q_income.reindex(columns=['Symbol', 'Income Item (MM)', '2015-09', '2015-12', '2016-03', '2016-06', '2016-09', 'TTM'])
        # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
        # set the index to Symbol for easy DB insertion
		ten_q_income.set_index(['Symbol'], inplace=True)

		return ten_q_income

	except Exception as e:
		return False, e


def create10QBalanceReport(symbol):
	"""

	Create a 10Q (quarterly) balance sheet for a given ticker.

	This function uses get10KQReport() to generate and clean a quarterly balance sheet report 
	for a given ticker. The field names in this report are shortened for readiblity. 
	This function takes a symbol argument that is a tuple of form ('SYMBOL','EXCHANGE'). 
	The symbol is any valid stock symbol. The exchange must be either 'NYSE' or 'NASDAQ'.

	Return value if successful is the TTM quarterly balance sheet packaged in a PANDAS dataframe. 
	Otherwise will return a tuple (False, error message). 

	Example Usage: crate10QBalanceReport(('GPRO','NASDAQ'))

	"""

	try:
		ten_q_balance = get10KQReport(symbol, 'bs', 3)
        # add symbol column and reorganize column headers
		ten_q_balance['Symbol'] = symbol[0]
        # rename columns where appropriate. Don't want to lose denomination. And don't want to lose column years b/c I can just keep adding to get my 10yr for free.
		ten_q_balance.rename(columns={'Fiscal year ends in December. USD in millions except per share data.':'Balance Item (MM)'}, inplace=True)
        # reindex the columns
		ten_q_balance = ten_q_balance.reindex(columns=['Symbol', 'Balance Item (MM)', '2015-09', '2015-12', '2016-03', '2016-06', '2016-09'])
        # # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
        # set the index to Symbol for easy DB insertion
		ten_q_balance.set_index(['Symbol'], inplace=True)

		return ten_q_balance
        
	except Exception as e:
		return False, e


def create10QCashflowReport(symbol):
	"""

	Create a 10Q (quarterly) cashflow report for a given ticker.

	This function uses get10KQReport() to generate a TTM quarterly cashflow sheet for the given symbol.
	The field names in this report are shortened for readiblity. This function takes a symbol argument 
	that is a tuple of form ('SYMBOL','EXCHANGE'). The symbol is any valid stock symbol. The exchange 
	must be either 'NYSE' or 'NASDAQ'.

	Return value if successful is the TTM quarterly cashflow sheet packaged in a PANDAS dataframe. 
	Otherwise will return a tuple (False, error message).

	Example Usage: crate10QCashflowReport(('GPRO','NASDAQ'))

	"""

	try:   
		ten_q_cashflow = get10KQReport(symbol, 'cf', 3)
        # add symbol column and reorganize column headers
		ten_q_cashflow['Symbol'] = symbol[0]
        # rename columns where appropriate. Don't want to lose denomination. And don't want to lose column years b/c I can just keep adding to get my 10yr for free.
		ten_q_cashflow.rename(columns={'Fiscal year ends in December. USD in millions except per share data.':'Cashflow Item (MM)'}, inplace=True)
        # reindex the columns
		ten_q_cashflow = ten_q_cashflow.reindex(columns=['Symbol', 'Cashflow Item (MM)', '2015-09', '2015-12', '2016-03', '2016-06', '2016-09', 'TTM'])
        # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
        # set the index to Symbol for easy DB insertion
		ten_q_cashflow.set_index(['Symbol'], inplace=True)

		return ten_q_cashflow
        
	except Exception as e:
		return False, e


def commitDividendHistory(data):
	'''

	Handles database commitment of dividend history for a stock.

	Not fully finished...this function recieves a pandas dataframe containing a stock's dividend 
	history and checks the Dividends table to make sure the history doesn't already exist. If it does, 
	it returns a message "already present." Otherwise, it commits the pandas dataframe to the database. 
	If you want to add more dividend information to the table for a stock, you will need to write a separate 
	function to handle that task.

	Returns message "already present" if dividend history exists. If not, it will commit the data
	to the Dividends table and return a tuple (True, 'success msg'). Any errors will be returned 
	as a tuple (False, error message)

	Example Usage: commitDividendHistory(dataframe)

	'''

	try: 
        # need to break, or at least just return a 'no value' msg, not raise value error. 
		if isinstance(data, str): # data is a string in this case, or if it's just a string.
            # now compare string value to 'No dividend history, if needed
			return data
        # check if the stock symbol is already present
		elif dividendHistoryExists(data.index[0]) == True:
			return 'Stock is already present. Try using updateDividends() instead, or delete the existing record.'
		else:
			data.to_sql('Dividends', con=cnx, if_exists='append')
			return (True, 'Successfully commited stock dividend history to the DB.')
    
	except Exception as e:
		return (False, e)


def dividendHistoryExists(symbol):
	"""

	Checks the Dividends table in the database to see if stock symbol is present.

	This is a gatekeeper function that will look up the Dividends table and find any matching 
	records of the symbol in the table's symbol field. Function can be used in any other function
	requing a check on the table before adding more data to Dividends. The symbol argument is 
	a string of any valid stock ticker, e.g., 'DUK'.

	Returns True if a record for the ticker exists. Returns tuple (False, 'No records') if no 
	records found. Any errors will be returned in a tuple (False, error message).

	Example Usage: dividendHistoryExists('DUK')

	"""

	try:
        # in future, add double check to make sure this symbol is even in the symbol list...faster lookup probably.
		if cur.execute('SELECT * FROM Dividends WHERE Symbol = ?', (symbol,)).fetchone() is not None:
			return True
        #otherwise return false
		return (False, 'No records found for {stock}.'.format(stock=symbol))
	except Exception as e:
		return (False, e)


def getDividendHistory(symbol, period):
	"""

	Downloads and formats an HTML dividend table packaged as a PANDAS dataframe. 

	Not fully finished...This function uses BeautifulSoup to gather the symbol's dividend history, 
	if any. The history is for cash dividends only. Upcoming dividends are included. The argument 
	symbol is a tuple ('SYMBOL', 'EXCHANGE') with any valid ticker and either NASDAQ or NYSE as the
	exchange. The period argument is the integer number of years for which dividend history is desired. 
	High numbers that surpass the available data (10Y) will default to supply all available. The returned 
	data will be formatted. Field names are shortened and any string numbers are converted to np.float64.

	The return value for this function will be 'no dividends' if there is no history. Otherwise, 
	return values will be either the pandas dataframe, or an error message of type tuple with the format
	(False, error message). 

	Note that there have been observed bugs, e.g., returning "ImportError('html5lib not found')" when 'SLB' 
	is entered as ticker, as well as some Unicode errors.

	Example Usage: getDividendHistory(('DUK','NYSE'), 10)

	"""

	try:
        # set flag to track stock's upcoming dividend status
		has_upcoming = False
        
        # specify the exchange
		exchange = stock_div_table_mngstar_exchange[0] if symbol[1] == 'NASDAQ' else stock_div_table_mngstar_exchange[1]
        # cast years as str just in case an int was passed
		years = str(period)
        # create the path to get the data
		upcoming_div_history_path = stock_div_table_mngstar_head + stock_div_table_mngstar_type[0] + stock_div_table_mngstar_action + exchange + symbol[0] + stock_div_table_mngstar_region + stock_div_table_mngstar_tail + years 
		div_history_path = stock_div_table_mngstar_head + stock_div_table_mngstar_type[1] + stock_div_table_mngstar_action + exchange + symbol[0] + stock_div_table_mngstar_region + stock_div_table_mngstar_tail + years 
        
        # get the data
		upcoming_raw_html = requests.get(upcoming_div_history_path).text # upcoming will eventually be its own function.
		past_raw_html = requests.get(div_history_path).text 
        
        # process the upcomming dividend table if there's any
		upcoming_soup = bsoup(upcoming_raw_html, 'lxml').find('table')
		upcoming_formatted_table = formatRawDivTable(upcoming_soup, 'upcoming')
              
        # get the past div table
		past_soup = bsoup(past_raw_html, 'lxml').find('table')        
		past_formatted_table = formatRawDivTable(past_soup, 'past')
    
        # process the historical dividend table if there's any
		if past_formatted_table == 'No Dividend': # check if empty
			return 'No dividend history for stock.'
        
        # if there's no data, flag it    
		if upcoming_formatted_table != 'No Upcoming':
			has_upcoming = True 
			upcoming_div_table = pd.read_html(str(upcoming_formatted_table), header=0, parse_dates=True, encoding='utf-8')
			upcoming_div_table = upcoming_div_table[0]    
      
        # pass the soup objects to pandas, using str as a backup measure to make sure to convert data to parsable format
		past_div_table = pd.read_html(str(past_formatted_table), header=0, parse_dates=True, encoding='utf-8')
        
        # since read_html returns a list, have to get the first element
		div_table = past_div_table[0]
        
        # merge the tables
		if has_upcoming == True:
			div_table = div_table.append(upcoming_div_table, ignore_index = True)
            
        # get a view of the div df where there are ONLY cash dividends
		div_table = div_table[div_table['Dividend Type'] == 'Cash Dividends'].copy()
        
        # set a symbol column
		div_table['Symbol'] = symbol[0]
        # reindex the columns, putting symbol at the front
		div_table = div_table.reindex(columns=['Symbol','Ex-Dividend Date','Declaration Date','Record Date','Payable Date','Dividend Type','Amount'])
        # set index to Symbol column for easy DB insertion
		div_table.set_index('Symbol', inplace=True)
        # check for stock splits or any numbers that don't fit the float format
        # clean up Amount column by removing $ sign and converting number to float
		div_table['Amount'] = div_table['Amount'].apply(lambda x: np.float64(re.sub('\$', '', x)))
        
		return div_table
    
	except Exception as e:
		return False, e


def formatRawDivTable(soup, which):
	"""

	Helper function formats html of upcoming and past dividends into a string that PANDAS can interpret.

	This function will create a properly formatted HTML table as a single string for PANDAS to read. 
	Beautiful Soup handles the extraction of the necessary info from the raw HTML that it receives from
	the soup argument. The which argument is a string of either 'upcoming' dividends or 'past'. The 
	default logic of which is to process past dividends.

	Returns 'no upcoming' to the calling function if there's no dividend data to get. Otherwise, will 
	return either the formatted html table, or a tuple with (False, error message) in case of error.

	Example Usage: formatRawDivTable(past_soup, 'past')

	"""

	try:        
        # make table end caps
		tbl_head = '<table>'
		tbl_tail = '</table>'
            
		if which == 'upcoming': # the upcoming div table
            # find the header row and convert to string
			header = str(soup.select('.gry')[0])
            # get the item that contains the needed data 
			contents = soup.find('tbody').contents
            
            # check to make sure there's content in the upcoming dividend table, otherwise flag and pass
			if len(contents) <= 3:
				return 'No Upcoming'
            
            # different binning for stocks paid semi-annually, annually, and quarterly            
			data_row = [] # container 
			for idx, item in enumerate(contents): # get the row we need
				if idx == 3: 
					data_row = str(item)
					pass # end the loop
			finished_table = tbl_head + header + data_row + tbl_tail # package the table
        
		else: # the past history table            
            # remove all thdr rows b/c they're not needed
			for row in soup.find_all('tr', class_='thdr'):
				row.decompose()
            # loop over rows and pass them to a new variable to extract tbodys and thead tags
			all_rows = []
			for row in soup.find_all('tr'):
				all_rows.append(str(row)) # make everything a string so join works
            # create a boolean check on the past_formatted_table to check for number of <tr> elements
			if len(all_rows) <= 1:
				return 'No Dividend'
            # join all rows
			all_rows = ''.join(all_rows)
            # put table together
			finished_table = tbl_head + all_rows + tbl_tail
        
		return finished_table
    
	except Exception as e:
		return False, e


def getStockFinancials(symbol):
	"""

	Retrieve a given stock's key financial ratios and package them in a PANDAS dataframe.

	This function builds a URL and fetches an individual stock's key performance ratios, 
	which tend to form a rather large table. The required symbol argument is of type tuple 
	('SYMBOL', 'EXCHANGE'). Most of these ratios can be calculated from the basic data 
	collected from 10K/Q and Price History reports. This function saves time and processing 
	power and is useful for tracking of more exotic ratios that might not be as important 
	to calculate on the fly in your algorithms.

	Returns a PANDAS dataframe if successful. Otherwise, returns a tuple (False, error message).

	Example Usage: getStockFinancials(('GPRO','NASDAQ'))

	"""

	try:
		exchange = stock_financials_mngstar_exchange[0] if symbol[1] == 'NASDAQ' else stock_financials_mngstar_exchange[1]
		stock_financials_path = stock_financials_mngstar_head + exchange + symbol[0] + stock_financials_mngstar_tail
		return pd.read_csv(stock_financials_path, header=2, encoding='utf-8')
    
	except Exception as e:
		return False, e


def createStockFinancialsReports(symbol):
	'''

	Gathers and formats the financial ratios for a stock into 3 separate PANDAS dataframes. 

	This function uses getStockFinancials() to acquire key financial ratios for a given stock. 
	The data fields are broken into three separate reports, each a PANDAS dataframe. The symbol 
	argument is a tuple with ('SYMBOL', 'EXCHANGE') which accepts any valid ticker symbol and 
	either 'NYSE' or 'NASDAQ'.

	If the data for a stock is already in the Financials table, the function returns 'already exists'. 
	Otherwise, returns 3 dataframes in a tuple (financials, growth_ratios, finhealth_ratios), 
	or an error message in tuple as (False, error message).

	Example Usage: createStockFinancialsReports(('DUK', 'NYSE'))

	'''

	try:
        
		if checkStockFinancialsExist(symbol[0])[0] == False: # func call indexed b/c it returns a tuple
            # get the raw data
			financials = getStockFinancials(symbol[0])
            # add Symbol column for tracking and adding to DB
			financials['Symbol'] = symbol[0]
            
            # rename one column for clarity
			financials.rename(columns={'Unnamed: 0': 'Measure'}, inplace=True)
            # create index key for column names
            # change the 10K and 10Q yr1 ..etc columns into pd.Timestamp for database storage
			col_order = ['Symbol', 'Measure', '2006-12', '2007-12', '2008-12','2009-12','2010-12','2011-12','2012-12','2013-12','2014-12', '2015-12', 'TTM']
            # rearrange the columns
			financials = financials.reindex(columns = col_order)
            
            # remove some unnecessary rows by slicing a view from the original data
			financials = financials[
									(financials['Measure'] != 'Margins % of Sales') &
									(financials['Measure'] != 'Key Ratios -> Profitability') &
									(financials['Measure'] != 'Profitability') &
									(financials['Measure'] != 'Key Ratios -> Cash Flow') &
									(financials['Measure'] != 'Cash Flow Ratios') & 
									(financials['Measure'] != 'Key Ratios -> Efficiency Ratios') & 
									(financials['Measure'] != 'Efficiency')
									]
            
            # indexes to use for slicing the growth rations 
			growth_index1 = financials[financials['Measure'] == 'Key Ratios -> Growth'].index
			growth_index1 = growth_index1[0] # remember: index the tuples
			growth_index2 = financials[financials['Measure'] == '10-Year Average'].index
			growth_index2 = growth_index2[-1]

            # indexes to use for slicing the financial health ratios
			finhealth_index1 = financials[financials['Measure'] == 'Key Ratios -> Financial Health'].index
			finhealth_index1 = finhealth_index1[0]
			finhealth_index2 = financials[financials['Measure'] == 'Debt/Equity'].index
			finhealth_index2 = finhealth_index2[0]
            
            # slice out growth ratios that have a different column for quarterly data
			growth_ratios = financials.ix[growth_index1:growth_index2].copy()
            # drop the first unwanted row
			growth_ratios = growth_ratios.iloc[2:].copy()
            # adjust the column name
			growth_ratios.rename(columns={'TTM': 'Latest Qtr'}, inplace=True)
            # set the index to the symbol for easy DB insertion
			growth_ratios.set_index('Symbol', inplace=True)

            # remove the financial health ratios from the financials frame
			finhealth_ratios = financials.ix[finhealth_index1: finhealth_index2].copy()
            # remove first two unneeded rows
			finhealth_ratios = finhealth_ratios.iloc[2:]
            # adjust the column name
			finhealth_ratios.rename(columns={'TTM': 'Latest Qtr'}, inplace=True)
            # set the index to the symbol for easy DB insertion
			finhealth_ratios.set_index('Symbol', inplace=True)      
            
            # drop the financial health ratios from the financial frame
			financials.drop(financials.index[finhealth_index1-5: finhealth_index2-4], inplace=True) # this index hack will have to improve eventually
            # drop the growth ratios from the financials frame
			financials.drop(financials.index[growth_index1-3 : growth_index2-2], inplace=True)
            # reset the index on the financials frame
			financials.set_index('Symbol',inplace=True)
            
			return (financials, growth_ratios, finhealth_ratios) # three data frames to go into DB 

		return 'already exists'
        
	except Exception as e:
		return False, e


def checkStockFinancialsExist(symbol):
	"""

	Gatekeeper function...checks if stock already has an entry in the financial ratio's table.

	This function looks through the financial_ratios table for the symbol. The symbol argument
	is a single string of any valid ticker symbol, e.g., 'DUK'. If the symbol is found in the 
	table, the function returns a tuple with (True, 'Success message'). If fasle, a tuple with 
	(False, 'No records'). Any errors will be returned in a tuple with (False, error message).

	Example Usage: checkStockFinancialsExist('DUK')

	"""

	try:
    	# if true, return True
		if cur.execute('SELECT * FROM financial_ratios WHERE Symbol = ? LIMIT 1', (symbol,)).fetchone() is not None:
			return (True, 'The {stock} already has a record.'.format(stock=symbol))
        #otherwise return False
		return (False, 'No records found for {stock}.'.format(stock=symbol))
	except Exception as e:
		return (False, e)


def commitStockFinancials(financial_reports):
	"""

	Saves the financial history dataframes to the db in their respective tables. 

	The financial_reports argument will be the return of createStockFinancialsReports(). 
	If that function returns 'already exists', commitStockFinancials() will return a message
	indicating the tables already exist in the DB. Otherwise, this function will 
	commit the the tuple of 3 dataframe objects to the 3 tables: financial_ratios, 
	finhealth_ratios, and growth_ratios.

	If successfully committed, the function will return a tuple (True, 'Success message'). 
	Any errors will be returned as a tuple with (False, error message).

	Example Usage: commitStockFinancials(createStockFinancialsReports(symbol))

	"""
	try: 
        # check if the stock symbol is already present
		if financial_reports == 'already exists': # financial_reports is a string if this condition is true
			return 'Stock financial ratios already comitted. Move along.'
		else:
            # otherwise, a dataframe is passed back
			financial_reports[0].to_sql('financial_ratios', con=cnx, if_exists='append')
			financial_reports[1].to_sql('finhealth_ratios', con=cnx, if_exists='append')
			financial_reports[2].to_sql('growth_ratios', con=cnx, if_exists='append')
			return (True, 'Successfully commited {stock} financial ratios to the DB.'.format(stock=financial_reports[0].index[0]))

	except Exception as e:
		return False, e


def populateAllFinancialReportsForStock(symbol):
	"""

	A helper function to populate all financial reports for all stocks listed in the DB in a single call.

	Still in development...this function is producing known errors on a few stocks such as SLB, GOOG.
	Note: This function only works after the initial Symbols table has been created from the NASDAQ. 
	This function initializes all data for all stock tickers listed in the symbols table in the database. 
	It processes the reports for that ticker, saving each report to the database. The symbol argument is 
	a string 'SYMBOL', which is used to complete a simple check on the database to ensure that the symbol 
	exists in the Symbols table.

	Returns a list of return values (success or failure) from all functions that are run. Otherwise,
	returns a tuple (False, error message).

	Example Usage: populateAllFinancialReportsForStock('GPRO')

	"""

	try:
        # check if the symbol exists in the db. can remove this same functionality from the methods to be called below
		db_symbol = cur.execute('SELECT * FROM all_stocks_key WHERE Symbol = ? LIMIT 1', (symbol,)).fetchone()
        
		if db_symbol[0] == symbol:
            
			success_msgs = []            
            
            # get 10yr price history and append result to success_msgs
			success_msgs.append( commitPriceHistory(createPriceHistoryReport(db_symbol)) )

            # get all 10Ks and append result to success_msgs
			success_msgs.append( commitFinancialsData(create10KIncomeReport(db_symbol), 'is', 12) ) # fail on SLB
			success_msgs.append( commitFinancialsData(create10KBalanceReport(db_symbol), 'bs', 12) )
			success_msgs.append( commitFinancialsData(create10KCashflowReport(db_symbol), 'cf', 12) )

            # get all 10Qs and append result to success_msgs
			success_msgs.append( commitFinancialsData(create10QIncomeReport(db_symbol), 'is', 3) ) # err on GOOG
			success_msgs.append( commitFinancialsData(create10QBalanceReport(db_symbol), 'bs', 3) )
			success_msgs.append( commitFinancialsData(create10QCashflowReport(db_symbol), 'cf', 3) ) # err on GOOG

            # get the dividend records, if any, and append result to success_msgs
			success_msgs.append( commitDividendHistory(getDividendHistory(db_symbol, '20')) )
            
            # get the financials records and append result to success_msgs
			success_msgs.append( commitStockFinancials(createStockFinancialsReports(db_symbol)) )
			success_msgs.append('Completed acquiring data for {stock}'.format(stock=symbol))
			return success_msgs
        
        # otherwise issue a value error
		raise ValueError('The stock symbol provided, {sym}, was not found in the database. Try again.'.format(sym=symbol))    
    
	except Exception as e:
		return (False, e)


def closeDBConnection():
	"""

	Remember that when you're done creating your database, you will want to call the closing methods 
	to commit all changes and close the connection. Failing to do so can sometimes have negative 
	consequences, as in the complete FUBARing of your database. 

	"""

	try:
		cnx.commit()
		cnx.close() # turn into DB function that opens and shots DB connection as needed.
		return True
	except Exception as e:
		return (False, e)


def timeDelayDataPopulate():
	"""

	Helper function...implements a simple random timer to iteratively process all stocks into the database.

	THis function controls all other functions to gather and process reports for all stocks 
	on the NYSE and NASDAQ. It selects all stock tickers from the database and iterates over 
	them using a random timer for each iteration to avoid slamming servers that provide the stock 
	data. It calls populateAllFinancialReportsForStock() on each iteration. Timer is set to 
	between 4-10 seconds and has been tested successfully. The drawback is that there are >5000 
	stocks to iterate over. So once you invoke this function, be prepared to work on another project 
	for some time. 

	Returns the return value of populateAllFinancialReportsForStock().

	Example Usage: timeDelayDataPopulate()

	"""
	try: 
		stocks = pd.read_sql('SELECT * FROM all_stocks_key', con=cnx).iloc[5001:, ]# get a slice of symbols to start with 
        
        #collect the results of populateAllFinanicalReportsForStock
		results = []
        
        # For future: create the all_stocks_key table or update it as necessary
        
        #loop over the stock symbols and get all data for each using a timer to avoid bombarding the servers
		for stock in stocks.iloc[0:,0]:
			wait_time = np.random.randint(4,10)
			time.sleep(wait_time) 
			results.append(populateAllFinancialReportsForStock(stock))

		closeDBConnection()

		return results

	except Exception as e: 
		return False, e


"""
Future work. 
============
Yahoo stuff
============

Update individual stocks using the Yahoo API, which is interfaced using the 
yahoo_finance module (pip install yahoo_finance).

"""

# END Module
