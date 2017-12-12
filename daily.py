# daily_stock_routines

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

# from selenium import webdriver # see if proper import resolves this dependency
# from selenium.common.exceptions import NoSuchElementException # does proper import resolve this dependency?
# from yahoo_finance import Share as yf # backup in case morningstar doesn't work. to do: add redundancy to data sources

## CUSTOM Modules
from core import coreStocks

class dailyStocks(coreStocks): # do i want to extend all functionality, or simply instantiate a core class inside of daily and use as needed?
    """
    Extends core functionality of stock data scraping by managing 
    the update of an existing DB of stocks traded on the NYSE and NASDAQ.

    """

    ## HELPER FUNCTIONS
    
    # dailyTimeDelayPriceUpdate()
    # *************************** #
    def dailyTimeDelayPriceUpdate(self, stocks): 
        # this could potentially take a few flags for monthly and quarterly update. 
        # but probably best to keep that functionality separate to make it easier to manage code base.
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

            # move check on trading day status to head of populate call 
            results = []

            # set start time for diagnostics
            results.append( 'Start time: ' + datetime.datetime.now().strftime("%Y:%B:%d:%I:%M:%S") + '\n')
            
            # get recent stock price updates 
            for stock in stocks.iterrows():
                wait_time = np.random.randint(4,10) # prevent slamming of servers with requests
                time.sleep(wait_time)
                print('Getting recent price history for {sym} - '.format(sym = stock[1][0]) + datetime.datetime.now().strftime("%I:%M:%S") + '\n') # print a helper message to console to show progress                
                # use getRecentMngStarPriceInfo to only commit new records
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
                symbol_changes = pd.read_html(full_path, header=0)[3] # index could change in future if html is restructured
                # concat all of the changes together
                if 'No records found.' not in symbol_changes.iloc[0][0]:
                    ticker_changes = pd.concat([ticker_changes, symbol_changes], ignore_index=True)
                else: break # drop out of loop if there's nothing left to capture

            # placeholder: as needed, clean up special chars
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
            # remove the snp sector allocation table
            db_tables.remove('SandPAllocation')
            
            # loop through DB tables and replace the old symbol with the new.
            success = [] # append a success message 
            for table in db_tables:
                for new_tick, old_tick in zip(changes['New'], changes['Old']):
                    self.dbcnx[1].execute("UPDATE '{tbl}' SET Symbol='{new}' WHERE Symbol='{old}';"\
                                    .format(tbl = table, new = new_tick, old = old_tick))
                    # set a flag to check for successful operation
                    success.append('Updated {old} with {new} in {tbl} table'.format(old=old_tick, new=new_tick, tbl=table))
            
            # redo: simplify messaging and return values by removing "updated" using an if.

            # check to make sure all messages were a success
            if len(success) and  all( ['Updated' in msg for msg in success ] ):
                return True, success
            else:
                return False, 'Nothing to update, or an error occured.', success

        except Exception as e:
            return False, e

    
    # updateAllStocksKey
    # 
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


    # to do...clean DB of old stocks that are no longer traded. Find them by checking 
    # for out of date price histories and getting the most recent dates that are older 
    # that a pre-set period of time, say 14 days.

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
                raise ValueError('%r is not in the price history database yet. Check to make sure stock symbol is correct or make call to getMngStarPriceInfo.' % stock)            

            # just get 10yr price history and use PANDAS to sort out dates you don't have yet.
            price_history = self.createPriceHistoryReport(stock)

            # some stock symbols are funds and what not, and they won't have price histories to update. skip these.
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
            
            # # filter out old dates
            mask = ( (price_history['Reference'] > last_date.iloc[0][0]) ) # & (price_history['Reference'] <= pd.to_datetime(today)) date range: +1 day since last update until today. use global "today" variable 
            price_history = price_history.loc[mask]

            # convert dates back to ISO formatted yyyy-mm-dd strings
            price_history['Reference'] = price_history['Reference'].dt.strftime('%Y-%m-%d')
            
            # # check for empty dataframe
            if price_history.index.size is 0: # this should never happen at this point, but you never know.
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
            
            # what if the new stocks and removed stocks are empty? 

            return old_stocks, new_stocks, removed_stocks
        
        except Exception as e: 
            return False, e   


    ## UNUSED ## Delete for migration to github

    # getYahooPriceInfo - redundancy in case MngStar does not work. Not finished.
    @staticmethod
    def getYahooPriceInfo(symbols):
        """
        Collects historical stock pricing info from Yahoo. 
        This function is a backup if getMngStarPriceInfo() does not 
        work, or if it returns a network error. 

        Takes a single argument of dataframe 
        Returns a dict of stock pricing info.
        """
        try: 
            # set up a dict to hold all the data
            yahoo_stock_info = {
                                'Symbol':[], #use a dict to initialize all values, which will be passed into a pandas dataframe
                                'Open': [],
                                'High':[], 
                                'Low':[], 
                                'Close':[],
                                'Volume':[],
                                }
                
            # YAHOO: if all conditions successful, get the symbol's data
            for key in symbols['Symbol'].iloc[0:1000]: # remove slice later to get all stocks and push them into DB.

                # checkTradingDay()
                wait_time = random.random() #np.random.randint(.1,.25)
                time.sleep(wait_time) # to avoid network 101 unavailable errs

                yahoo_stock_info['Symbol'].append(key)
                yahoo_stock_info['Open'].append( yf(key).get_open() )
                yahoo_stock_info['High'].append( yf(key).get_days_high() )
                yahoo_stock_info['Low'].append( yf(key).get_days_low() )
                yahoo_stock_info['Close'].append( yf(key).get_price() )
                yahoo_stock_info['Volume'].append( yf(key).get_volume() )

            # convert data to dataframe
            yahoo_stock_info = pd.DataFrame(yahoo_stock_info)
            # reindex the dataframe
            yahoo_stock_info = yahoo_stock_info.reindex_axis(['Symbol','Open','High','Low','Close','Volume'], axis=1)
            # convert all prices to floats
            columns = ['Open', 'High', 'Low', 'Close'] 
            yahoo_stock_info[columns] = yahoo_stock_info[columns].astype(float)
            # convert volume to int
            yahoo_stock_info['Volume'] = yahoo_stock_info['Volume'].astype(int)
            # add a date column 
            yahoo_stock_info['Reference'] = pd.to_datetime(datetime.date.today())

            return yahoo_stock_info

        except Exception as e:
            return (False, e)

    # checkTradingDay
    # *************** #

    @staticmethod
    def checkTradingDay(today = datetime.date.today()):
        """
        Helper function checks if today's date is a valid day to get pricing info from the markets. 
        Unnecessary function that will be removed in future versions. 

        You can simply run price updates on any day using getRecentMngStarPriceInfo() which uses 
        PANDAS date filtering from the DB's previous date to today to avoid unnecessary computation.
        
        Accepts a datetime.date object. If none provided, defaults to today().

        Returns either True if today is a valid day, otherwise False
        """
        try:
            # set dates for comparison
            # yesterday_query = dbmgt.dbcnx[1].execute("SELECT Reference FROM TenYrPrices ORDER BY DATE(Reference) DESC LIMIT 1;").fetchone() # if one date is valid, all dates shold be.
            # yesterday = datetime.datetime.strptime(yesterday_query[0], '%Y-%m-%d %H:%M:%S').date()
            
            day = today.isoweekday()

            # check today is not a Sunday day
            if day > 5: #see if the update has already been made today by checking the date
                if day == 6: 
                    # if yesterday.isoweekday() < 5 ... Friday has not been added yet
                    return True, 'Today is a Saturday. Starting download for Friday\'s market.'
                    # else: return False, 'Today is a weekend and last Friday's market has already been added. See you next trading day.'  
                else:
                    return False, 'Stocks not updated on Sundays. See you next trading day.'
            
            # check the weekday is not a trading holiday
            elif today in holidays.US():
                return False, 'Today is a US holiday. Stocks not updated. See you next trading day.'

            else: # Today is trading day
                return True, '{date} is a trading day. Starting download for today\'s market.'.format(date= str(today))

        except Exception as e:
            return (False, e)

    # checkTradingTime()
    # ****************** # 
    @staticmethod
    def checkTradingTimeEnded(right_now = datetime.datetime.now(tz=pytz.timezone('America/New_York'))):
        """
        Unnecessary function to be removed. Simply use getRecentMngStarPriceInfo()
        instead, which uses PANDAS filtering to get the previous date up to most 
        recent history. 

        Checks to make sure that the current US EST is b/t 8:30p to 4a the next day
        to make sure all market data (NY and Chicago) will be available. 
        
        NASDAQ operating hours 4a (premarket) to 8P (aftermarket)
        NYSE operating hours 9:30a to 4pm
        
        Daemon will only execute one time per day, or attempt 3 other tries if there's a fault such as 
        a network connection issue. Daemon will attempt to update @8:30p EST or later (if there's an 
        issue updating on the first try).
        
        Takes a datetime.date argument of the current datetime. If none provided, defaults to today()

        Returns True if trading has ended and data available. Otherwise returns a tuple of False, error message.
        """
        try:
            
            # create the UTC
            # UTC_TZ = pytz.utc
            EASTERN_TZ = pytz.timezone('America/New_York')
            
            # clarify the date for the prices.
            if right_now.hour <= 1: # subtract one day from date
                right_now -= datetime.timedelta(days=1)
                
            # create the 19:30 UTC time stamp for market closing
            closing = datetime.datetime.combine(datetime.date.today(), datetime.time(19, 30, tzinfo=EASTERN_TZ))
            
            if right_now.hour >= closing.hour or right_now.hour <= 2: # anytime b/t the safe limits of 8:59p and 2:59a
                return True, 'Ready to download the {daily} trading day stock prices.'.format(daily = str(right_now.date()))
            else:
                return False, 'It\'s not time to update the {today} stock price. Try again b/t 9p to 2:59a US EST.'.format(today=str(right_now.date()))

        except Exception as e:
            return False, e

    # updateStockPrices()
    # ******************* # 
    def checkUpdateStatus(self):
        """
        Handles the logic for determining the update status of the stock db's price 
        history. This function will only work at the end of the trading day between 
        the hours of 5p and 6:59a the next morning.

        Function will be eliminated as it is unnecessary. getRecentMngStarPriceInfo()
        updates to current day all the dates I don't have using PANDAS filtering.

        Returns True if stock prices can be updated, otherwise False and error message.
        """
        try:
            # make sure the markets are closed
            day_status = self.checkTradingDay()
            time_status = self.checkTradingTimeEnded()  
            
            if day_status[0] is False:
                return day_status
            elif time_status[0] is False:
                return time_status
            # all clear
            return time_status
        
        except Exception as e:
            return False, e
