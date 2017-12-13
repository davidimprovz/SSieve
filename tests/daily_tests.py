# daily_tests.py

# Exercise the methods of the coreStocks class
# each time a change is made to the code base.

import os, sys, re, pprint, time, datetime, holidays
sys.path.append('../')

import pandas as pd
import numpy as np

import sqlite3
import pandas.io.sql as pdsql

import urllib
import requests
from bs4 import BeautifulSoup as bsoup

import daily


def dailyStocksTests():
	"""
	Exercises the daily scraping daemon module consisting
	of the coreStocks class and several helper methods.

	Steps
	----- 
	1. instantiate class, set variables, and connect to a DB instance.
	2. get and test the current NASDAQ downloaded stock list.
	3. rename tickers in the DB as needed using renameStocks.
	4. update price history for all unchanged stocks.
	5. update the DB AllStocksKey table with any new tickers using compareStockListsWithIsIn.
	6. use the new_stocks dataframe to timeDelayPopulate each one.
	7. close the DB to commit all changes.

	"""
	try:

		# STEP 1: instantiate class, set variables, connect to DB instance. 

		test_db = os.getcwd() + '/db/daily_tests_dummy.db' # for testing w/ data
		exchanges = ('NASDAQ', 'NYSE')

		# make a DB folder in directory if doesn't already exist
		directory = os.getcwd() + '/db'
		if not os.path.exists(directory):
			os.makedirs(directory)

		# instantiate dailyStocks class, connect to db, and test
		update_stocks = daily.dailyStocks(test_db)
		update_stocks.connectToDB(update_stocks.dbcnx)
		assert isinstance(update_stocks, daily.dailyStocks), "Failed to instantiate dailyStocks"
		assert isinstance(update_stocks.dbcnx[0], sqlite3.Connection), "failed to connect to DB with message: %r" % update_stocks.dbcnx[0]
		# print db path for reference
		print('\n' + update_stocks.dbcnx[2])
		
		


		# STEP 2: get and test the current NASDAQ downloaded stock list. 

		# test stock list instance type and contents
		stock_list = update_stocks.getAllCurStocks(exchanges)
		sl_expected_cols = ['Symbol', 'Name', 'LastSale', 'MarketCap', 'IPOyear', 'Sector', 'Industry', 'Market', 'MarketCapSym']
		assert isinstance(stock_list, pd.DataFrame), "NASDAQ stock list is not a dataframe: returned %r" % type(stock_list)
		assert stock_list.index.size > 1, "NASDAQ stock list was empty."
		assert len(stock_list.columns) == len(sl_expected_cols), "NASDAQ stock list missing columns: got %r" % len(stock_list.columns)
		assert all(i in stock_list.columns for i in sl_expected_cols), "NASDAQ stock list columns don't match: got %r" % stock_list.columns



		# STEP 3: rename tickers in the DB as needed using renameStocks.

		ticker_changes = update_stocks.checkStockNameChanges()
		assert isinstance(ticker_changes, pd.DataFrame), "Expected stock name changes to return pandas DataFrame. Got %r instead" % type(tickers)
		assert ticker_changes.index.size > 1, "Stock name change dataframe was empty."
		
		changes_made = update_stocks.renameStocks(ticker_changes) 
		
		if changes_made[0] is True: 
			assert( all( ['Updated' in msg for msg in changes_made[1]] ) ), "all stock name changes were reported good but did not process properly."
		elif changes_made[0] is False:
			if 'Nothing to update' in changes_made[1]:
				print(changes_made)



		# STEP 4: update price history for all unchanged stocks.

        # query dummy DB for old stocks if one exists
		old_stocks = pd.read_sql('SELECT * FROM "AllStocksKey";', con=update_stocks.dbcnx[0])
		assert isinstance(old_stocks, pd.DataFrame), "The stock list retrieved from the DB was empty."

		# get references to old, new and removed stocks
		comparisons = update_stocks.compareStockListsWithIsIn(old_stocks, stock_list)
		# make sure each element in comparisions is a dataframe
		assert len(comparisons) is 3, "compareStockListsWithIsIn supposed to return tuple of 3 elements. Got %r instead." % len(comparisons)
		assert all(isinstance(i, pd.DataFrame) for i in comparisons), "compareStockListsWithIsIn failed to return all dataframes." 

		# test creation of most recent price history for a stock from the DB picked at random
		available_symbols = pd.read_sql('SELECT Symbol FROM TenYrPrices;', con=update_stocks.dbcnx[0]) # get symbols from old DB TenYrPrices instead of AllStocksKey
		available_symbols.drop_duplicates(inplace=True)	
		# get randomly selected stock 
		random_num = np.random.randint(0, available_symbols.index.size)
		random_stock = available_symbols.iloc[random_num]
		random_stock = pd.read_sql('SELECT * FROM AllStocksKey WHERE Symbol = "{sym}";'.format(sym=random_stock[0]), con=update_stocks.dbcnx[0])
		# get a most recent history report
		pr_hist_report = update_stocks.getRecentMngStarPriceInfo( (random_stock.iloc[0][0], random_stock.iloc[0][1]) )
		pr_hist_cols = ['Reference', 'Open', 'High', 'Low', 'Close', 'Volume']
		
		if isinstance(pr_hist_report, tuple): 
			print(pr_hist_report[1])
		else:
			assert isinstance(pr_hist_report, pd.DataFrame), "Formatted price history report not a dataframe: returned %r" % type(pr_hist_report)
			assert pr_hist_report.index.size > 0, "Formatted price history report was empty."
			assert len(pr_hist_report.columns) == len(pr_hist_cols), "Formatted price history report missing columns: got %r" % len(pr_hist_report.columns)
			assert all(i in pr_hist_report.columns for i in pr_hist_cols), "Formatted price history report columns don't match: got %r" % pr_hist_report.columns

		# test commit of daily price history and query DB for the table for a preselected stock
		pr_hist_commit = update_stocks.commitPriceHistory(pr_hist_report, True)
		assert isinstance(pr_hist_commit, tuple), "Price history commit failed to return expected value. Expected tuple. Got %r instead" % type(pr_hist_commit)
		assert pr_hist_commit[0] is True, "Price history update failed with message: %r" % pr_hist_commit[1]

		# set today's date and check the most recent commit to ensure it was pushed to DB
		test_retrieve =  pd.read_sql('SELECT * FROM TenYrPrices WHERE Symbol = "{sym}" ORDER BY date(Reference) DESC Limit 1;'.format(sym=random_stock.iloc[0][0]), con=update_stocks.dbcnx[0])
		assert isinstance(test_retrieve, pd.DataFrame), "Updated price history from DB not a dataframe: returned %r" % type(test_retrieve)
		assert test_retrieve.index.size > 0, "Updated price history from DB is empty."

		# test dailyTimeDelayPriceUpdate for an old stock picked at random
		random_stocks = comparisons[0].sample(2)
		results = update_stocks.dailyTimeDelayPriceUpdate(random_stocks)
		assert all('Successfully' in i[1] for i in results), "Some price histories failed: %r" % results
		
		

		# STEP 5: update the DB AllStocksKey table with any new tickers using compareStockListsWithIsIn.

		# now update the AllStocksKey db table
        new_stocks = comparisons[1] # note: shouldn't be any new if test is run 2x.

		all_stocks_update = update_stocks.updateAllStocksTable(new_stocks)
		assert isinstance(all_stocks_update, tuple), "updateAllStocksTable returned an unexpected type. Got %r" % type(all_stocks_update)
		if all_stocks_update[0] is False:
			print('\n')
			print("AllStocksKey was not updated. Got msg: %r" % all_stocks_update[1])



		# STEP 6: use the new_stocks dataframe to timeDelayPopulate each one.
	
		# finally, add a newly reported stock to the DB	
		new_additions = update_stocks.timeDelayDataPopulate(new_stocks)	
		for i in new_additions: 
		 	print('\n')
		 	print(i)
	


		# STEP 7: close the DB to commit all changes.
		

		print('\n' + str(update_stocks.closeDBConnection(update_stocks.dbcnx[0])))
		
		# delete db if not using dummy data DB 
        os.remove(test_db) # comment out to turn off for data you want to keep

		return 'Tests completed. No errors to report.'

	except Exception as e:
		return ('\n!!!! A problem occured !!!!\n', e, '\n')

if __name__ == '__main__':
	message = dailyStocksTests()

	if isinstance(message, tuple):
		for i in message:
			print (i)
	else: print('\n' + message + '\n')
