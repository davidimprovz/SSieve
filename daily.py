# encapsulates daily_stock_routines

## PACKAGES Import 
import sys, os
import sqlite3 
import pytz, time, datetime, holidays 
import requests
import re
import random
import numpy as np
import pandas as pd
import pandas.io.sql as pdsql

## CUSTOM Modules
from core import coreStocks


class dailyStocks(coreStocks):
    """
    Extends core functionality of stock data scraping by managing 
    the update of an existing DB of stocks traded on the NYSE and NASDAQ.

    """

    ## HELPER FUNCTIONS
    
    # dailyTimeDelayPriceUpdate()
    # *************************** #
    def dailyTimeDelayPriceUpdate(self, stocks): 
        """
        Helper function which implements a simple random timer to iteratively udpate all stocks in the database.

        This function selects all stock tickers from the database and iterates over 
        them using a random timer for each iteration to avoid slamming servers that provide the stock 
        data. It calls getRecentMngStarPriceInfo() on each stock.
        
        The timer is set to between 4-10 seconds and has been tested successfully. The drawback is that 
        there are >5000 stocks to iterate over. So once you invoke this function, be prepared to work on 
        another project for some time. 
        
        This function will later implement an async call to. It might be best to deploy over a cloud 
        infrastructure where each node could make only a few requests.

        The argument 'stocks' is a PANDAS dataframe to iterate over with the columns 'Symbol' and 'Exchange', 
        although a series of just 'Symbol' would also work.

        Returns a dictionary of the success state of each {'Sym':'Update status'} pair from getRecentMngStarPriceInfo().

        Example Usage: dailyTimeDelayPriceUpdate(existing_stocks)
        """
        try:     
            assert isinstance(stocks, pd.DataFrame), "dailyTimeDelayPriceUpdate accepts a pandas DataFrame argument. Got %r instead." % type(stocks)
            assert stocks.columns.size is 2, "The stocks dataframe for dailyTimeDelayPriceUpdate should only have two columns. Got %r instead." % stocks.columns.size
 
            results = []
            # set start time for diagnostics
            results.append( 'Start time: ' + datetime.datetime.now().strftime("%Y:%B:%d:%I:%M:%S") + '\n')
            
            # get recent stock price updates 
            for stock in stocks.iterrows():
                wait_time = np.random.randint(4,10) # prevent slamming of servers with requests
                time.sleep(wait_time)
                print('Getting recent price history for {sym} - '.format(sym = stock[1][0]) + datetime.datetime.now().strftime("%I:%M:%S") + '\n') # print message to console to show progress
                results.append(self.commitPriceHistory(self.getRecentMngStarPriceInfo( (stock[1][0], stock[1][1]) ), daily=True))
            
            # set end time for diagnostics
            results.append( 'End time: ' + datetime.datetime.now().strftime("%Y:%B:%d:%I:%M:%S") )

            return results

        except Exception as e: 
            return False, e


    ## OLD STOCKS

    # checkStockNameChanges()
    # *********************** #
    @staticmethod
    def checkStockNameChanges():
        """
        Handles the renaming of stocks in the DB that have been renamed. It will switch 
        both the name and the symbol for every table in the database. 

        Uses PANDAS to capture html tables from the NASDAQ listings containing symbol changes. 

        Returns the changes in a PANDAS frame, if any. If problem scraping NASDAQ, returns error msg.
        If code error, returns a tuple of False, error.
        """
        try:

            path = 'http://www.nasdaq.com/markets/stocks/symbol-change-history.aspx?page='
            ticker_changes = pd.DataFrame()
            for i in np.arange(100): # set the number high enough to catch all pages
                page = str(i+1)
                full_path = ''.join([path, page])
                symbol_changes = pd.read_html(full_path, header=0)[3] # note: index could change in future if html is restructured
                # concat all of the changes together
                if 'No records found.' not in symbol_changes.iloc[0][0]:
                    ticker_changes = pd.concat([ticker_changes, symbol_changes], ignore_index=True)
                else: break # drop out of loop if there's nothing left to capture

            ticker_changes.rename(columns={'Old Symbol': 'Old', 'New Symbol': 'New', 'Effective Date': 'Date'}, inplace=True)

            # check returned value
            assert isinstance(ticker_changes, pd.DataFrame), "Expected stock name changes to return pandas DataFrame. Got %r instead" % type(tickers)

            return ticker_changes

        except Exception as e:
            return False, e

    # renameStocks()
    # ************** # 
    def renameStocks(self, changed_tickers):
        """
        Handles the renaming of stock tickers in the DB. It will switch only the ticker, not
        the company name, for every table in the database.

        Accepts pandas dataframe of changes to tickers with the columns 'Old', 'New', and 'Date'.

        Returns True if the update was successful. Otherwise, returns tuple False, e.
        
        Example usage: renameStock(changes)
        """
        try:
            # report if there was an error running checkStockNameChanges(), which supplies the changed_tickers argument.
            assert isinstance(changed_tickers, pd.DataFrame), "Wrong argument type provided. Takes a pandas DataFrame. Got a %r instead." % type(changed_tickers)
            
            # lookup key
            old_db_stocks = pd.read_sql('SELECT * FROM AllStocksKey;', con=self.dbcnx[0])
            # find all ticker changes where symbol is in existing db of stocks
            changes = changed_tickers.where(changed_tickers['Old'].isin(old_db_stocks['Symbol'])).dropna() # discard NANs
            # lookup for all DB tables while removing nested tuples
            db_tables = [i[0] for i in self.dbcnx[1].execute('SELECT name FROM sqlite_master WHERE type="table";').fetchall()]
            
            # loop through DB tables and replace the old symbol with the new.
            success = [] # append a success message 
            for table in db_tables:
                for new_tick, old_tick in zip(changes['New'], changes['Old']):
                    self.dbcnx[1].execute("UPDATE '{tbl}' SET Symbol='{new}' WHERE Symbol='{old}';"\
                                    .format(tbl = table, new = new_tick, old = old_tick))
                    # set a flag to check for successful operation
                    success.append('Updated {old} with {new} in {tbl} table'.format(old=old_tick, new=new_tick, tbl=table))
        
            # check to make sure all messages were a success
            if len(success) and  all( ['Updated' in msg for msg in success ] ):
                return True, success
            else:
                return False, 'No ticker changes to make, or an error occured.', success

        except Exception as e:
            return False, e

    
    # updateAllStocksKey
    # ****************** #
    def updateAllStocksTable(self, new_stocks):
        """
        Receives a pandas DataFrame with two columns: new stock tickers and exchange. 
        
        Checks the AllStocksKey table for the new stocks and adds them to the db 
        if they don't exist.
        """

        try: 
            assert isinstance(new_stocks, pd.DataFrame), "The updateAllStocksTable argument must be a dataframe. Got a %r." % type(new_stocks)
            assert new_stocks.columns.size is 2, "The argument should have only 2 columns. Received %r instead." % new_stocks.columns.size
            
            # check if new symbols exist in the DB and were somehow skipped.
            existing_stocks = pd.read_sql('SELECT * FROM AllStocksKey', con=self.dbcnx[0])
            no_update = new_stocks.where(new_stocks.Symbol.isin(existing_stocks.Symbol)).dropna()
            to_update = new_stocks.where(~new_stocks.Symbol.isin(existing_stocks.Symbol)).dropna()

            # return if no new stocks..this means there was an error in the sorting of compareStocksWithIsIn.
            existing = no_update.index.size
            new = to_update.index.size

            # return True for updates
            if new > 0:
                # add the symbols and return success message
                to_update.to_sql('AllStocksKey', con=self.dbcnx[0], if_exists='append', index=False)

                # report on existing stocks that weren't updated
                if existing > 0:
                    return True, 'Added {new} records and ignored {old} records.'.format(new=new, old=existing)
                else: 
                    return True, 'Added %r records.' % new
            
            else:
                return False, 'No new records to add.'

        except Exception as e:
            return False, e

    # getRecentMngStarPriceInfo
    # ************************* # 
    def getRecentMngStarPriceInfo(self, stock):
        """
        Makes network calls to morningstar to acquire all of the pricing data for a stock 
        already in the DB. Use Mng Star as the primary source because it requires one network call 
        to acquire all 5 pricing data features per stock.

        Accepts a single stock argument, which is a tuple of (symbol, exchange).

        Returns a PANDAS datframe of pricing info if successful. Otherwise False, error.
        """
        try:
            record = pd.read_sql('SELECT DISTINCT Symbol FROM TenYrPrices WHERE Symbol="{sym}";'.format(sym=stock[0]), con=self.dbcnx[0])
            if record.empty:
                return '%r is not in the price history database yet. Check to make sure stock symbol is correct or make call to getMngStarPriceInfo.' % stock            

            # just get 10yr price history and use PANDAS to sort out dates you don't have yet.
            price_history = self.createPriceHistoryReport(stock)

            # some stock symbols are funds and as such won't have price histories to update. skip these.
            
            # do nothing if the data returned is empty. Probably means a network issue or some old stock made it past my filter, or was recently suspended, and needs to be removed manually.
            if not isinstance(price_history, pd.DataFrame) or price_history.index.size == 0: # index condition may never be true with a symbol column
                return 'No price history available for {symbol}'.format(symbol=stock[0])
            # use dates to filter missing dates from today back to the last date in the db
            # sorting string dates sqlite3 and return only 1 record
            last_date = pd.read_sql( 'SELECT Reference FROM TenYrPrices WHERE Symbol = "{sym}" ORDER BY date(Reference) DESC Limit 1;'.format(sym=stock[0]), con=self.dbcnx[0])
            last_date['Reference'] = pd.to_datetime(last_date['Reference']) # convert to datetime    
            # convert the price_history dates to pandas datetime and sort descending
            price_history['Reference'] = pd.to_datetime(price_history['Reference']) # convert dates here. Why? See comment on csv call.
            price_history.sort_values(['Reference'], ascending=False, inplace=True)
            
            # filter out old dates
            mask = ( (price_history['Reference'] > last_date.iloc[0][0]) )
            price_history = price_history.loc[mask]

            # convert dates back to ISO formatted yyyy-mm-dd strings
            price_history['Reference'] = price_history['Reference'].dt.strftime('%Y-%m-%d')
            
            # check for empty dataframe
            if price_history.index.size is 0: # this should never happen at this point, but just to be sure.
                return False, 'You already have the latest pricing info or there was an unlikely error.'

            return price_history

        except Exception as e:
            return False, e


    ## NEW STOCKS

    # compareStockListsWithIsIn()
    # *************************** #
    @staticmethod
    def compareStockListsWithIsIn(db_list, new_list):
        """
        Compare stock lists to isolate old stocks in the DB from new stocks recently added to the exchanges.
        
        Split the new_stocks into two sets: those which are still in the DB, and those which are new. 
        Comparison is accomplished by merging two dataframes on the columns querying the set.

        Accepts two dataframes as arguments to compare. The old_stocks is the list of all stocks currently
        in the DB. new_stocks is a list of stocks currently traded on the NASDAQ. The shape of these frames is a
        two-column frame with the names 'Symbol' and 'Market'.

        Returns two dataframes: new_symbols is all traded stock symbols not currently in the database with 
        their cooresponding exchanges. same_symbols is the compliment, all stocks with symbols in the db
        which are also valid symbols in the new_stocks list passed in as an argument.
        
        Returned value can be:
        1) new and not in DB - if true, a report can be generated, and will be listed in nasdaq
        2) old and not in Exchange - if true, no more reports can be generated, and will be listed in nasdaq
        3) symbol change and in Exchange - if true, a report can be generated, will be listed in nasadq, and will match existing report
        4) symbol change and not in Exchange - if true, no more reports and will be listed in nasdaq
        """
        try:
            assert isinstance(db_list, pd.DataFrame), "Wrong argument type provided to compareStockListsWithIsIn. Takes a pandas DataFrame. Got a %r instead." % type(db_list)
            assert isinstance(new_list, pd.DataFrame), "Wrong argument type provided to compareStockListsWithIsIn. Takes a pandas DataFrame. Got a %r instead." % type(new_list)

            new_stocks = new_list.where(~new_list['Symbol'].isin(db_list['Symbol'])).dropna()
            old_stocks = new_list.where(new_list['Symbol'].isin(db_list['Symbol'])).dropna()
            removed_stocks = db_list.where(~db_list['Symbol'].isin(new_list['Symbol'])).dropna()
            
            mask = ['Symbol', 'Market']
            new_stocks = new_stocks[mask]
            old_stocks = old_stocks[mask]
            removed_stocks = removed_stocks[mask]
        
            return old_stocks, new_stocks, removed_stocks
        
        except Exception as e: 
            return False, e
