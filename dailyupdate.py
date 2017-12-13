#dailyupdate.py
"""
Script to update stock prices in database on a daily basis after the closing 
of the markets and before next-day opening.
"""

## PACKAGES import

import os, sys, re, pprint, time, datetime, holidays
sys.path.append('../')

import pandas as pd
import numpy as np

import sqlite3
import pandas.io.sql as pdsql

import urllib
import requests
from bs4 import BeautifulSoup as bsoup

# custom classes
import dbmgt
import core
import daily


def doDailyUpdate(directory, db_name):
	"""
	Control logic for daily stock price and ticker symbol updates.

	If successful, returns messages of updated stocks and new stocks data 
	acquisition status. Otherwise, will return tuple of Flase and error.

	Steps
	----- 
	1. instantiate class, set variables, and connect to a DB instance.
	2. get the current NASDAQ downloaded stock list.
	3. rename tickers in the DB as needed using renameStocks.
	4. update price history for all unchanged stocks.
	5. update the DB AllStocksKey table with any new tickers using compareStockListsWithIsIn.
	6. use the new_stocks dataframe to timeDelayPopulate each one.
	7. close the DB, commit all changes, and log results.

	"""
	try:

		# STEP 1: SET VARIABLES AND CONNECT TO DB
		
		# instantiate dailyStocks class and connect to db
		db_path = ''.join([directory, db_name])
		dailys = daily.dailyStocks(db_path)
		# connect to DB
		dailys.connectToDB(dailys.dbcnx)
		# challenge db connection
		assert isinstance(dailys.dbcnx[0], sqlite3.Connection), 'Couldn\'t connect to DB %r. Try again.' % db_name
		# print db path to console for reference
		print(dailys.dbcnx[2])



		# STEP 2: get and test the current NASDAQ downloaded stock list. 

		# first get the current list of stocks
		exchanges = ['NASDAQ', 'NYSE']
		# test stock list instance type and contents
		stock_list = dailys.getAllCurStocks(exchanges)



		# STEP 3: rename tickers in the DB as needed using renameStocks.
		
		# get a dataframe of all stock name changes from NASDAQ listings
		ticker_changes = dailys.checkStockNameChanges()
		# change the ticker symbols
		changes_made = dailys.renameStocks(ticker_changes) 
		# if issues, print status of stocks that have been changed to console 
		if changes_made[0] is True: 
			assert( all( ['Updated' in msg for msg in changes_made[1]] ) ), "All stock name changes were reported good but did not process properly."
		elif changes_made[0] is False:
			if 'Nothing to update' in changes_made[1]:
				print(changes_made[1])

		

		# STEP 4: update price history for all unchanged stocks.
		
		# get a reference for all stocks currently in DB
		db_stocks = pd.read_sql('SELECT * FROM "AllStocksKey";', con=dailys.dbcnx[0])
		# split old, new and unused tickers into separate dataframes
		comparisons = dailys.compareStockListsWithIsIn(db_stocks, stock_list)
		# get the new prices for each stock and keep track of the results of the acquisition
		price_hist_results = dailys.dailyTimeDelayPriceUpdate(comparisons[0])
    

		
		# STEP 5: update the DB AllStocksKey table with any new tickers using compareStockListsWithIsIn results.

		# report success updateAllStocksTable
		all_stocks_update = dailys.updateAllStocksTable(comparisons[1])
    


		# STEP 6: timeDelayPopulate new stocks
	
		# finally, add a newly reported stock to the DB	
		new_additions = dailys.timeDelayDataPopulate(comparisons[1])	
		

		# STEP 7: close db, commit changes, and log results
		
		# close the db connection before exiting
		dailys.closeDBConnection(dailys.dbcnx[0])

		# log all results....to do - set programatically
		log = '/home/dev/AUTOSIFT/ez_equity_daemon/scraping/output/daily/results_' + datetime.date.today().strftime("%B_%d_%Y") + '.txt'  # use ''.join([]) constructor
            with open(log, 'w') as f:
            
                f.write('\n\n----- NAME CHANGES -----\n\n')
                
                for i in changes_made:
                    
                    f.write(str(i))
                    
                    f.write('\n')

                f.write('\n\n----- PRICE HISTORY -----\n\n')

                for elem in price_hist_results:
                    f.write(str(elem[0]))
                    for i in elem[1:-1]:
                        f.write(str(i))
                        f.write('\n')
                    f.write(str(elem[-1]))

                f.write('\n\n----- ALL STOCKS KEY -----\n\n')
                for i in all_stocks_update:
                    f.write(str(i))
                    f.write('\n')

                f.write('\n\n----- NEW STOCKS -----\n\n')

                for elem in new_additions:
                    f.write(str(elem[0]))
                    for i in elem[1:-1]:
                        f.write(str(i))
                        f.write('\n')
                    f.write(str(elem[-1]))

                f.write('\n\n----- END -----\n\n')
            f.close()

		return True, 'Finished daily updates. Check log file for results at %r' % log
		
	except Exception as e:
		return False, e


if __name__ == '__main__':
	
	try:
		# check to make sure cmd line argument passed
		assert len(sys.argv) >= 3, "Missing an argument. Please supply a db directory and file name to write to with the format NAME.db"
		# make sure a DB folder in directory if doesn't already exist
		if os.path.exists(sys.argv[1]) is False:
			raise ValueError('An invalid directory name was passed to daily update function. Check for errors in %r' % sys.argv[1])
		# check to make db name passed has proper format
		if '.db' not in sys.argv[2]:
			raise ValueError('No db connection passed with script. Try again.')
		# initialize the db
		results = doDailyUpdate(sys.argv[1], sys.argv[2])
		print(results)

	except Exception as e:
		print('An error occured.', e)
		
