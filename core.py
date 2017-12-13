# timeless routines

# PACKAGE Imports
import sys, os
import time, datetime, holidays

import urllib
import requests

import sqlite3 

import numpy as np
import pandas as pd
import pandas.io.sql as pdsql

from bs4 import BeautifulSoup as bsoup

# CUSTOM Modules
from globalvars import accessStrings
from dbmgt import stockDB


# notes #
# remember to close DB Connection properly b/f exiting to avoid random unwanted behavior

class coreStocks(stockDB, accessStrings):
    """
    See readme file for more details on this class.
    
    Per stockDB's __init__ method, 
    you must pass in a db connection when instantiating this method.

    Don't forget to establish a DB connection after you intantiate this class.
    """

    # HELPER Functions

    # alignReportColumns
    # ****************** #
    @staticmethod
    def alignReportColumns(sheet):
        """
        Helper function that works with format10KQSheet and createStockFinancialsReports
        to align column names to allow for setting the index of the sheet to the symbol of 
        the stock.

        Takes a single dataframe argument, which is some finanical report whose columns 
        need to be realigned with the Symbol column at the head. 

        Returns the dataframe passed in with columns realigned. Otherwise False, error tuple
        """
        try:
            if isinstance(sheet, pd.DataFrame):
                # create new index by moving the Symbol col from the end to the beginning
                cols = [i for i in sheet.columns.insert(0, sheet.columns[-1])]
                # remove the extra entry for Symbol column
                cols.pop(-1)
                # reindex
                sheet = sheet.reindex(columns=cols)

                return sheet
            else: 
                raise ValueError( 'Variable with incorrect data type passed. A dataframe is required but a {kind} was passed'.format(kind=type(sheet)) )
        
        except Exception as e:
            return False, e

    # cleanNullColumns
    # **************** #
    @staticmethod
    def cleanNullColumns(sheet):
        """
        Helper function to discard columns in sheets where each value in column is null.

        Accepts a DataFrame as the sheet argument.

        Returns the cleaned dataframe or an error Tuple of (False, error)
        """
        try:# check for and remove columns with all NaNs
            for column in sheet.columns: 
                if pd.isnull(sheet[column]).all():
                    sheet.drop(column, axis=1, inplace=True)
            return sheet
        
        except Exception as e:
            return False, e

    @staticmethod
    def removeColumnSpaces(sheet):
        """
        Format a column name to remove spaces.

        Takes dataframe as argument.

        Returns pandas dataframe with edited column names (where applicable)
        """

        return sheet.rename(columns=lambda x: x.replace(' ', '_'))

    # timeDelayDataPopulate
    def timeDelayDataPopulate(self, stocks):
        """
        Helper function...implements a simple random timer to iteratively process all stocks into the database.

        This function controls all other functions to gather and process reports for all stocks 
        on the NYSE and NASDAQ. It selects all stock tickers from the database and iterates over 
        them using a random timer for each iteration to avoid slamming servers that provide the stock 
        data. It calls populateAllFinancialReportsForStock() on each iteration. Timer is set to 
        between 4-10 seconds and has been tested successfully. The drawback is that there are >5000 
        stocks to iterate over. So once you invoke this function, be prepared to work on another project 
        for some time. 

        Accepts stocks argument, which is a dataframe of stocks to iterate over. This dataframe should have
        the columns 'Symbol' and 'Exchange', although a series of just 'Symbol' would also work.

        Returns the return value of populateAllFinancialReportsForStock().

        Example Usage: timeDelayDataPopulate()
        """
        try: 
            
            assert isinstance(stocks, pd.DataFrame), "timeDelayDataPopulate expected a dataframe argument. Got %r" % type(stocks)
            if stocks.index.size is 0:
                return "Empty dataframe passed to timeDelayDataPopulate. No new stocks to get."

            results = [] # collect results for log

            # set start time for diagnostics
            results.append( 'Start time: ' + datetime.datetime.now().strftime("%Y:%B:%d:%I:%M:%S") + '\n')
            
            # loop over stock symbols and get data for each. Use timer to avoid server throttle.    
            
            for stock in stocks.iterrows():
                wait_time = np.random.randint(4,10)
                time.sleep(wait_time)
                print('Gathering data on {sym} - '.format(sym = stock[1][0]) + datetime.datetime.now().strftime("%I:%M:%S") + '\n') # print a helper message to console to show progress
                results.append(self.populateAllFinancialReportsForStock(stock[1])) # iterating over rows in series...must select second elem in tuple
            
            # acquire the snp allocation for the entire market
            results.append( self.commitSandP(self.getSandPAllocation(), True) )
            # set end time for diagnostics
            results.append( 'End time: ' + datetime.datetime.now().strftime("%Y:%B:%d:%I:%M:%S") )

            return results

        except Exception as e: 
            return False, e

    # populateAllFinancialReportsForStock
    def populateAllFinancialReportsForStock(self, stock, daily=False):
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

        Example Usage: populateAllFinancialReportsForStock(stocks) where stocks is a pandas DataFrame
        with two columns, 'Symbol' and 'Market'.
        """
        try:

            # issue a warning and stop network calls if the symbol doesn't exist
            if daily is False and self.symbolExists(stock[0]) != True:
                    raise ValueError( 'The stock symbol passed does not match one in the DB. Update the DB or correct your symbol, %r.' % stock[0] )
            
            success_msgs = []                            
            # add a routine to capture stock symbols that are no longer traded referenced from price history report. 
            # eliminate the symbol, from future searches and updates (but not from the DB).
            success_msgs.append('Start data gather for {stock} - '.format(stock=stock[0]) + datetime.datetime.now().strftime("%I:%M:%S") )
           
            # get 10yr price history and append result to success_msgs
            success_msgs.append( self.commitPriceHistory( self.createPriceHistoryReport(stock) ) )
            # get all 10Ks and append result to success_msgs
            success_msgs.append( self.commitFinancialsData( self.create10KIncomeReport(stock), 'is', 12) )
            success_msgs.append( self.commitFinancialsData( self.create10KBalanceReport(stock), 'bs', 12) )
            success_msgs.append( self.commitFinancialsData( self.create10KCashflowReport(stock), 'cf', 12) )
            # get all 10Qs and append result to success_msgs
            success_msgs.append( self.commitFinancialsData( self.create10QIncomeReport(stock), 'is', 3) )
            success_msgs.append( self.commitFinancialsData( self.create10QBalanceReport(stock), 'bs', 3) )
            success_msgs.append( self.commitFinancialsData( self.create10QCashflowReport(stock), 'cf', 3) )
            # get the dividend records, if any, and append result to success_msgs
            success_msgs.append( self.commitDividendHistory( self.getDividendHistory(stock, '20')) )
            # get the financials records and append result to success_msgs
            success_msgs.append( self.commitStockFinancials( self.createStockFinancialsReports(stock) ) )
            
            success_msgs.append('End data gather for {stock} - '.format(stock=stock[0]) + datetime.datetime.now().strftime("%I:%M:%S") )
            return success_msgs
        
        except Exception as e:
            return (False, e)

    # makeStockListURL
    # **************** #
    def makeStockListURL(self, exchange='NASDAQ'):
        """
        Creates a URL for PANDAS to download a CSV list of all current stocks from NASDAQ.com

        Argument exchange is a string of either 'NYSE' or 'NASDAQ' for the exchange, which it uses
        to combine the URL path to the csv file. Defaults to NASDAQ if no exchange specified.

        Returns the complete URL as a string.

        Example Usage: makeStockListURL('NYSE')
        """
        try: 
            the_exchange = self.all_cur_stocks_csv_exchange[0]    
            if exchange.lower() == 'nyse': 
                the_exchange = self.all_cur_stocks_csv_exchange[1]
            return ''.join([self.all_cur_stocks_csv_base_url, the_exchange, self.all_cur_stocks_csv_tail])

        except Exception as e:
            return False, e



    # REPORTS

    # getAllCurStocks
    # *************** #
    def getAllCurStocks(self, exchanges):
        """
        Convenience function for donwloading and cleaning the csv file of all stocks from NASDAQ and NYSE.

        The function takes either a list or tuple of len = 2, consisting of the strings 'NYSE' and 'NASDAQ'.
        It calls the function that makes the URL for downloading the data, and then retreives the data.
        It also performs several cleanup functinos on the data, converting numerical strings to floats, and
        putting data in a more manageable format.

        Returns a single dataframe of all stocks traded on the exchanges requested.

        Example Usage: 
            current_stocks = getAllCurStocks(['NASDAQ', 'NYSE'])
            createSymbolsKeyTable(current_stocks[['Symbol', 'Market']]) # uses only the Symbol and Market 
                field names in the returned dataframe to create the table
        """
        try:
            #download all the stocks from NASDAQ and NYSE
            stock_lists_to_download = [self.makeStockListURL(exchanges[0]), self.makeStockListURL(exchanges[1])]
            exchange_data = [pd.read_csv(i, index_col = 0, encoding='utf-8') for i in stock_lists_to_download]
            #make column in each frame for Exchange and assign the market that the stock belongs to
            for idx, i in enumerate(exchange_data): 
                i.loc[:,'Market'] = 'NASDAQ' if idx == 0 else 'NYSE' 
            #merge data into single frame
            all_exchanges = pd.concat([exchange_data[0], exchange_data[1]])
            # drop the Unnamed and Summary Quote columns
            all_exchanges.drop(['Unnamed: 8', 'Summary Quote'], axis=1, inplace=True)
            #drop all n/a(s) in the LastSale column b/c I don't care about stock that's not selling.
            all_exchanges = all_exchanges[ (all_exchanges.loc[:,'LastSale'] != 'n/a') & (all_exchanges.loc[:, 'LastSale'] != None) ]
            # cast all numeric values in LastSale as float instead of string
            all_exchanges.loc[:, 'LastSale'] = all_exchanges.loc[:,'LastSale'].astype(float)
            #add column for marketcap symbol and remove all symbols and numbers from marketcap that to get the multiplier
            all_exchanges['MarketCapSym'] =  all_exchanges['MarketCap'].replace('[$0-9.]', '', regex=True)
            #remove $ and letters from MarketCap fields
            all_exchanges['MarketCap'] = all_exchanges['MarketCap'].replace('[$MB]', '', regex=True)
            all_exchanges.reset_index(inplace=True)
            #remove any unwanted whitespace from symbol or name
            all_exchanges['Symbol'] = all_exchanges['Symbol'].replace('\s+', '', regex=True)
            #replace all n/a values in MarketCap with np.NAN
            all_exchanges[all_exchanges['MarketCap'] == 'n/a'] = np.NAN
            #convert MarketCap to a float.
            all_exchanges['MarketCap'] = all_exchanges['MarketCap'].astype(float)
            #round the LastSale column
            all_exchanges['LastSale'] = all_exchanges['LastSale'].round(2)
            #rename industry column
            all_exchanges.rename(columns={'industry':'Industry'}, inplace=True)
            all_exchanges = all_exchanges[all_exchanges['Symbol'].notnull()] 
            # remove any duplicate stock symbols using pandas unique() method
            all_exchanges.drop_duplicates(subset='Symbol', keep='first', inplace=True)
            
            return all_exchanges
        
        except Exception as e:
            return (False, e)

    # createPriceHistoryReport
    # ************************ #
    def createPriceHistoryReport(self, stock):
        """
        Calls get10YrPriceHistory() to package a price history report into a PANDAS dataframe, then cleans and returns the data.

        This function will acquire a price history for the provided symbol, which must be a string and a valid stock symbol
        along with the symbol's exchange, e.g., ('MMM', 'NYSE'). The get10YrPriceHistory() function requires the exchange.
        
        After the data is loaded, the function adds a Symbol field to the price history for tracking in the database, reindexes 
        and renames some fields, properly formats the dates into datetime fields, and converts prices from strings to floats.

        Returns the report as a PANDAS dataframe if successful, otherwise a tuple (False, error message).

        Example Usage: createPriceHistoryReport(('MMM', 'NYSE'))
        """
        try:
            # get the raw data from morningstar    
            price_history = self.get10YrPriceHistory(stock)
            
            if isinstance(price_history, pd.DataFrame): # the price_history has to exist, or else return the err msg of the function called
                
                price_history['Symbol'] = stock[0]
                # reorganize header order
                price_history = price_history.reindex(columns=['Symbol','Date','Open','High','Low','Close','Volume'])
                # rename the Date column for easier processing through SQLite's Date functionality
                price_history.rename(columns={'Date':'Reference'}, inplace=True)
                # convert all dates to ISO formatted yyyy-mm-dd strings
                price_history['Reference'] = price_history['Reference'].apply(lambda x: time.strftime("%Y-%m-%d", time.strptime(x, "%m/%d/%Y")))
                
                # convert volumes to integers # unicode err on ??? value for some volumes goes to NaN

                price_history['Volume'] = pd.to_numeric(price_history['Volume'].str.replace(',',''), errors='coerce')
                # set index b/f db commit so no duplicate numeric index columns
                price_history.set_index(['Symbol'], inplace=True)
            
            return price_history

        except Exception as e:
            return (False, e)

    # get10YrPriceHistory
    # ******************* #
    def get10YrPriceHistory(self, symbol):
        """
        Get 10Y price history, one symbol at a time.

        Function takes two arguments.
        
        symbol argument is a single stock symbol and it's exchange in the form of an iterable with two strings. 
        That symbol is used to build a URL path to collect the 10Y price history as a CSV. The data is loaded 
        into a PANDAS dataframe.
        
        daily argument is a flag for triggering a simple report over YTD time period instead of for a 10y period.
        
        Returns a pandas dataframe if successful. Otherwise, returns a tuble of (False, error message).

        Example usage: get10YrPriceHistory(('ULTI', 'NASDAQ'))
        """
        try:
            exchange = self.stock_price_mngstar_csv_exchange[0] if symbol[1] == 'NASDAQ' else self.stock_price_mngstar_csv_exchange[1]    
            
            price_history_path = (self.stock_price_mngstar_csv_base_url + 
                                    exchange + symbol[0] +
                                    self.stock_price_mngstar_csv_period[0] +
                                    self.stock_price_mngstar_csv_freq_str +
                                    self.stock_price_mngstar_csv_freq_period[0] + 
                                    self.stock_price_mngstar_csv_tail)

            # throws EmptyDataError('No columns to parse from file') if nothing returned
            price_history = pd.read_csv(price_history_path, header=1, encoding = 'utf8') # header is on second row
            
            if not isinstance(price_history, pd.DataFrame):
                raise ValueError('Price history report failed. No dataframe returned. Got %r.' % price_history )
            
            return price_history
        
        except Exception as e: 
            
            return False, e, 'There is no price history for {stock}. The stock may no longer be traded, or it is so new that there is no price report available for 10yr period.'.format(stock=symbol[0])

    # getDividendHistory
    # ****************** #
    def getDividendHistory(self, symbol, period):
        """
        Downloads and formats an HTML dividend table packaged as a PANDAS dataframe. 

        Unlike most report gathering functions, this one does not have a "createXXReport() method. 
        Instead, the getDividendHistory() method accomplishes all of this in one pass. 
        The reason is that we are using BeautifulSoup instead of PANDAS to gather the data.

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
            exchange = self.stock_div_table_mngstar_exchange[0] if symbol[1] == 'NASDAQ' else self.stock_div_table_mngstar_exchange[1]
            # cast years as str just in case an int was passed
            years = str(period)
            # create the path to get the data
            upcoming_div_history_path = ''.join([self.stock_div_table_mngstar_head, self.stock_div_table_mngstar_type[0], self.stock_div_table_mngstar_action, exchange, symbol[0], self.stock_div_table_mngstar_region, self.stock_div_table_mngstar_tail, years]) 
            div_history_path = ''.join([self.stock_div_table_mngstar_head, self.stock_div_table_mngstar_type[1], self.stock_div_table_mngstar_action, exchange, symbol[0], self.stock_div_table_mngstar_region, self.stock_div_table_mngstar_tail, years]) 
            
            # get the data
            upcoming_raw_html = requests.get(upcoming_div_history_path).text
            past_raw_html = requests.get(div_history_path).text 
            
            # process the upcomming dividend table if there's any
            upcoming_soup = bsoup(upcoming_raw_html, 'lxml').find('table')
            upcoming_formatted_table = self.formatRawDivTable(upcoming_soup, 'upcoming')
                  
            # get the past div table
            past_soup = bsoup(past_raw_html, 'lxml').find('table')        
            past_formatted_table = self.formatRawDivTable(past_soup, 'past')
        
            # process the historical dividend table if there's any
            if past_formatted_table == 'No Dividend': # check if empty
                return 'No dividend history for stock.'
            
            # if there's no data, flag it    
            if upcoming_formatted_table != 'No Upcoming':
                has_upcoming = True 
                upcoming_div_table = pd.read_html(str(upcoming_formatted_table), header=0, parse_dates=True, encoding='utf-8')
                upcoming_div_table = upcoming_div_table[0]    
          
            # pass the soup objects to pandas, using str as a backup measure to make sure to convert data to parsable format
            past_div_table = pd.read_html(str(past_formatted_table), header=0, parse_dates=True, encoding='utf-8')[0] # since read_html returns a list, get the first element
            
            # merge the tables
            if has_upcoming == True:
                div_table = past_div_table.append(upcoming_div_table, ignore_index = True)
            else:
                div_table = past_div_table.copy()

            # set a symbol column
            div_table['Symbol'] = symbol[0]
            # reindex the columns, putting symbol at the front
            div_table = div_table.reindex(columns=['Symbol','Ex-Dividend Date','Declaration Date','Record Date','Payable Date','Dividend Type','Amount'])
            # set index to Symbol column for easy DB insertion
            div_table.set_index('Symbol', inplace=True)
            # check for stock splits or any numbers that don't fit the float format
            
            # account for payment in different currency adding a currrency column
            div_table['Currency'] = div_table['Amount'].str.extract('([A-Z]*)', expand=False)
            # remove any remaining whitespace
            div_table['Amount'] = div_table['Amount'].replace('(/\s/g)?([A-Z]?)','',regex=True)
            # clean up Amount column by removing $ sign and converting number to float
            div_table['Amount'] = div_table['Amount'].replace('\$', '', regex=True)
            # replace spaces with underscores for sqlite3 compatability
            div_table = self.removeColumnSpaces(div_table)

            return div_table
        
        except Exception as e:
            return False, e

    # formatRawDivTable
    # ***************** #
    @staticmethod
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

            if soup is None:
                return 'No Dividend'

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
                for idx, item in enumerate(contents): # get the needed row
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

    # getStockFinancials
    # ****************** #
    def getStockFinancials(self, symbol):
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
            exchange = self.stock_financials_mngstar_exchange[0] if symbol[1] == 'NASDAQ' else self.stock_financials_mngstar_exchange[1]
            
            stock_financials_path = self.stock_financials_mngstar_head + exchange + symbol[0] + self.stock_financials_mngstar_tail
            raw_financials = pd.read_csv(stock_financials_path, header=2, encoding='utf-8')
            
            return raw_financials
        
        except Exception as e:
            empty_msg = 'No available financial information for {equity}.'.format(equity=symbol[0]) 
            
            if isinstance(e, pd.io.common.CParserError):
                return empty_msg
            elif isinstance(e, pd.io.common.EmptyDataError):
                return empty_msg
            else:
                return False, e

    # createStockFinancialsReports
    # **************************** #
    def createStockFinancialsReports(self, symbol):
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
        
            if self.checkStockFinancialsExist(symbol[0])[0] == False: # func call indexed b/c it returns a tuple
                # get the raw data and return an err msg if no data available
                financials = self.getStockFinancials(symbol)
                if 'No available' in financials:
                    return financials

                # add Symbol column for tracking and adding to DB
                financials['Symbol'] = symbol[0]

                # rename one column for clarity
                financials.rename(columns={'Unnamed: 0': 'Measure'}, inplace=True)

                # realign the columns to put the Symbol first
                financials = self.alignReportColumns(financials)
                
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

                finhealth_ratios.rename(columns={'TTM': 'Latest Qtr'}, inplace=True)
                # set the index to the symbol for easy DB insertion
                finhealth_ratios.set_index('Symbol', inplace=True)      
                
                # drop the financial health ratios from the financial frame
                financials.drop(financials.index[finhealth_index1-5: finhealth_index2-4], inplace=True)
                
                # drop the growth ratios from the financials frame
                financials.drop(financials.index[growth_index1-3 : growth_index2-2], inplace=True)
                # reset the index on the financials frame
                financials.set_index('Symbol',inplace=True)
                # remove spaces in column names for sqlite3 compatability
                financials = self.removeColumnSpaces(financials)

                # discard any columns with all null values
                growth_ratios = self.cleanNullColumns(growth_ratios)
                finhealth_ratios = self.cleanNullColumns(finhealth_ratios)
                financials = self.cleanNullColumns(financials)
                

                return (financials, growth_ratios, finhealth_ratios) # three data frames to go into DB 

            return 'already exists'
            
        except Exception as e:
            return False, e

    # get10KQReports
    # ************** #
    def get10KQReport(self, symbol, report_type, freq):
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
            exchange = self.mngstar_fin_csv_exchange[0] if symbol[1] == 'NASDAQ' else self.mngstar_fin_csv_exchange[1]
            frequency = self.mngstar_fin_csv_report_freq_str[0] if freq == 3 else self.mngstar_fin_csv_report_freq_str[1]
            report = None
            if report_type == 'is': 
                report = self.mngstar_fin_csv_report_type[0]
            elif report_type == 'bs': 
                report = self.mngstar_fin_csv_report_type[1]
            else: 
                report = self.mngstar_fin_csv_report_type[2]
            report_path = (self.mngstar_fin_csv_base_url + exchange + 
                           symbol[0] + self.mngstar_fin_csv_report_region +
                           report + self.mngstar_fin_csv_report_period + 
                           frequency + self.mngstar_fin_csv_tail)
            
            data = pd.read_csv(report_path, header=1, encoding='utf-8') # header is on second row. remove first. 
            
            return data
            
        except Exception as e:
            if isinstance(e, pd.io.common.EmptyDataError):
                return 'No 10KQ {report} report available for {stock}.'.format(report=report_type.upper(), stock=symbol[0])
            else:
                return False, e

    # format10KQSheet
    # *************** #
    def format10KQSheet(self, sheet, symbol, report):
        """
        Helper function that works with get10KQReport to format financial data. 

        Accepts three arguments. sheet is the return value of get10KQReport. 
        symbol is the tuple ('SYMBOL', 'MARKET') passed with get10KQReport.
        sheet_type is the report being generated, which will allow for properly 
        labeled column names.

        Returns the formatted report in a dataframe to the calling function, or a tuple False, error.
        """
        try:
            assert report in ['is','bs','cf'], "Unknown report formatting requested. Expected is, bs, or cf but got %r" % report
            
            # check for and remove columns with all NaNs
            sheet = self.cleanNullColumns(sheet)
            # add symbol column
            sheet['Symbol'] = symbol[0]

            # replace 1st column containing "Fiscal year ends".
            col = sheet.columns[0]
            assert 'Fiscal' in col, "Warning: The first column to be formatted in this sheet did not contain a reference to the fiscal year. Got %r instead." % col
            
            if report is 'is':
                sheet.rename(columns={col:'Income item'}, inplace=True)
            elif report is 'bs': 
                sheet.rename(columns={col:'Balance item'}, inplace=True)
            else: #report is 'cf'
                sheet.rename(columns={col:'Cashflow item'}, inplace=True)

            # remove spaces in all columns so sqlite3 commit doesn't issue warning.
            sheet = self.alignReportColumns(self.removeColumnSpaces(sheet))
            # set symbol as index for storage
            sheet.set_index(['Symbol'], inplace=True)

            return sheet

        except Exception as e:
            return False, e

    # create10KIncomeReport
    # ********************* #
    def create10KIncomeReport(self, symbol):
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
            ten_k_income = self.get10KQReport(symbol, 'is', 12) # note: a slow connection prevents download
            
            if isinstance(ten_k_income, pd.DataFrame): # if no error downloading info for a new stock or simply initializing the db            
                ten_k_income = self.format10KQSheet(ten_k_income, symbol, 'is')

            return ten_k_income

        except Exception as e:
            return (False, e)

    # create10KBalanceReport
    # ********************** #
    def create10KBalanceReport(self, symbol):
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
            ten_k_balance = self.get10KQReport(symbol, 'bs', 12) # note: a slow connection prevents download
            
            if isinstance(ten_k_balance, pd.DataFrame): # if no error downloading info for a new stock or simply initializing the db            
                ten_k_balance = self.format10KQSheet(ten_k_balance, symbol, 'bs')

            return ten_k_balance
            
        except Exception as e:
            return False, e

    # create10KCashflowReport
    # *********************** #
    def create10KCashflowReport(self, symbol):
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
            ten_k_cashflow = self.get10KQReport(symbol, 'cf', 12) # note: a slow connection prevents download
            
            if isinstance(ten_k_cashflow, pd.DataFrame): # no error downloading info for a new stock or simply initializing the db
                 ten_k_cashflow = self.format10KQSheet(ten_k_cashflow, symbol, 'cf')

            return ten_k_cashflow
            
        except Exception as e:
            return False, e

    # create10QIncomeReport
    # ********************* #
    def create10QIncomeReport(self, symbol):
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
            ten_q_income = self.get10KQReport(symbol, 'is', 3)
            
            if isinstance(ten_q_income, pd.DataFrame): # if error downloading info for a new stock or simply initializing the db
                ten_q_income = self.format10KQSheet(ten_q_income, symbol, 'is')
                
            return ten_q_income

        except Exception as e:
            return False, e

    # create10QBalanceReport
    # ********************** #
    def create10QBalanceReport(self, symbol):
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
            ten_q_balance = self.get10KQReport(symbol, 'bs', 3)

            if isinstance(ten_q_balance, pd.DataFrame): # if error downloading info for a new stock or simply initializing the db
                ten_q_balance = self.format10KQSheet(ten_q_balance, symbol, 'bs')

            return ten_q_balance
            
        except Exception as e:
            return False, e

    # create10QCashflowReport
    # *********************** #
    def create10QCashflowReport(self, symbol):
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
            ten_q_cashflow = self.get10KQReport(symbol, 'cf', 3)

            if isinstance(ten_q_cashflow, pd.DataFrame): # if error downloading info for a new stock or simply initializing the db
                ten_q_cashflow = self.format10KQSheet(ten_q_cashflow, symbol, 'cf')

            return ten_q_cashflow
            
        except Exception as e:
            return False, e

    # createSymbolsKeyTable
    # ********************* # 
    def createSymbolsKeyTable(self, symbols):
        """
        Initializes the xchanges DB by creating all needed tables for all stocks listed in NASDAQ and NYSE.

        This function receives a PANDAS dataframe with fields stock symbol and exchange.
        If the table is added to the DB correctly, returns True. If table already exists, 
        a ValueError is returned with False in a tuple. 

        Example Usage: createSymbolsKeyTable( getCurrentStocks() )
        """
        try: # create the key symbols table
            assert isinstance(symbols, pd.DataFrame), "Requires a Dataframe as argument. Got %r instead." % type(symbols)   
        
            if self.symbolTableExists() == True:
                raise ValueError("The Symbols key table already exists. Move along.")
            symbols.to_sql('AllStocksKey', con=self.dbcnx[0], if_exists='replace', index=False)
            return True

        except Exception as e:
            return (False, e)


    # DB LOGIC & MGT

    # commitPriceHistory
    # ****************** # 
    def commitPriceHistory(self, data, daily=False):
        """
        Commits a stock's price history dataframe to the database. 

        This function receives a dataframe and will check to see if a 10Y price history for the stock it references.
        Note that the history checking routine only looks to see that the referenced stock already exists in the 
        price history table. If so, it will report a ValueError. 

        If you want to do daily updates of stock prices, use True for the daily argument.

        Returns a tuple (True, 'Success Message') if successful. Otherwise, returns a tuple (False, error message)

        Example Usage: commitPriceHistory(data)
        """
        
        try:
            # return a 'no value' msg, not raise value error.
            if isinstance(data, str) and 'No' in data:
                return False, data
            
            # pass on get[Recent]MngStarPrice error messages and failures to get price histories
            if isinstance(data, tuple) and data[0] is False: # the only condition that can occure from getMngStarPrice...
                return data

            # catch the case where daily update returns no new information
            if daily is True: 
                if isinstance(data, tuple) and 'You already have the latest' in data[1]:
                    return data

            # catch if there is no known error but a dataframe didn't get passed
            if not isinstance(data, pd.DataFrame):
                return 'Requires a pandas dataframe. Got a {instance}.'.format(instance=type(data))

            # if this is a completely new entry, make sure it's new
            if daily is False:
                # check if the stock symbol is already present
                if self.priceHistoryExists(data.index[0]) == True:
                    raise ValueError("The symbol is already present. Try using updateStockPrice() instead, or delete the existing record.")        

            # otherwise, add new columns if needed to DB
            self.checkAndAddDBColumns(data.columns,'TenYrPrices')

            # then post all new records to the table
            data.to_sql('TenYrPrices', con=self.dbcnx[0], if_exists='append')

            return (True, 'Successfully commited {stock} price history to the DB.'.format(stock=data.index[0]))    
        
        except Exception as e:
            return False, e
            
    # commitDividendHistory
    # ********************* # 
    def commitDividendHistory(self, data, monthly=False):
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
            # if 'no value' msg recieved
            if isinstance(data, str): 
                if 'No dividend' in data: 
                    return False, data 

            if monthly is False:
                # check if stock symbol is already present
                if self.dividendHistoryExists(data.index[0]) is True:
                    return 'Stock is already present. Use monthly=True flag with this method to update the DB, or delete the existing record.'

            data.to_sql('Dividends', con=self.dbcnx[0], if_exists='append')
            return (True, 'Successfully commited stock dividend history to the DB.')
            
        except Exception as e:
            return (False, e)
            
    # commitStockFinancials
    # ********************* # 
    def commitStockFinancials(self, financial_reports):
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
            if isinstance(financial_reports, str): # financial_reports is a string if this condition is true
                if 'No available' in financial_reports:
                    return False, financial_reports
                else: return 'Stock financial ratios already comitted. Move along.'

            # add new columns, if needed
            self.checkAndAddDBColumns(financial_reports[0].columns,'FinancialRatios')
            self.checkAndAddDBColumns(financial_reports[1].columns,'FinHealthRatios')
            self.checkAndAddDBColumns(financial_reports[2].columns,'GrowthRatios')

            # otherwise, a dataframe is passed back
            financial_reports[0].to_sql('FinancialRatios', con=self.dbcnx[0], if_exists='append')
            financial_reports[1].to_sql('FinHealthRatios', con=self.dbcnx[0], if_exists='append')
            financial_reports[2].to_sql('GrowthRatios', con=self.dbcnx[0], if_exists='append')
            
            return True, 'Successfully commited {stock} financial ratios to the DB.'.format(stock=financial_reports[0].index[0])

        except Exception as e:
            return False, e

    # commitFinancialsData 
    # ******************** # 
    def commitFinancialsData(self, report, report_type, report_period):
        """
        Handles commitment of 10K/Q reports to the database in their respective tables.

        This function will commit a generated financial report to DB and create the appropriate table if it doesn't exist.
        The required report argument is the dataframe created by get10KQReport(). The stock symbol included is used to 
        check whether the financial history for this stock is present in this report_type's table. Report_type 
        consists of a string with options of 'is', 'bs', and 'cf' for income, balance, and cashflow sheets. Report_period 
        is an integer of either 3 or 12 for 3-month and 12-month.

        Returns True if the commit was successful, otherwise it will return a tuple (False, ValuerError or other exception).

        Note: PANDAS will implicitly set up tables. No need to write separate funcs to set up those tables or specify col names.

        Example Usage: commitFinancialsData(report_df, 'bs', 12)

        """
        try:    
            # catch if there's a string that says "no history", etc. must come first to avoid indexing error
            if isinstance(report, str): # financial_reports is a string if this condition is true
                if 'No' in report:
                    return False, report
            
            if not isinstance(report, pd.DataFrame): # no errors retreiving data
                # pass an error back to the calling function
                raise ValueError("Got wrong data type to commit to DB. Report was a %r" % type(report))

            # see if the stock symbol exists and raise an error if so.
            if self.financialHistoryExists(report.index[0], report_type, report_period) is True: # must specify if true
               raise ValueError('Error: There\'s already a record matching this one. Try using commitIndividualFinancials() method to update the financial info instead.')            

            # sort by report type
            if report_type == 'is':
                if report_period == 3: 

                    # add columns if needed
                    # known issues in this code...must have consistent naming of columns
                    self.checkAndAddDBColumns(report.columns,'TenQIncome')
                    report.to_sql('TenQIncome', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenQIncome report to the DB.'
                    # clean_df_db_dups()
                elif report_period == 12: # report goes into annuals
                    self.checkAndAddDBColumns(report.columns,'TenKIncome')
                    report.to_sql('TenKIncome', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenKIncome report to the DB.'
                else: # catch formatting error  
                    raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))                
            
            elif report_type == 'bs':
                if report_period == 3:
                    self.checkAndAddDBColumns(report.columns,'TenQBalance')
                    report.to_sql('TenQBalance', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenQBalance report to the DB.'
                elif report_period ==12: 
                    self.checkAndAddDBColumns(report.columns,'TenKBalance')
                    report.to_sql('TenKBalance', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenKBalance report to the DB.'
                else: 
                    raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))
           
            elif report_type == 'cf':
                if report_period == 3:
                    self.checkAndAddDBColumns(report.columns,'TenQCashflow')
                    report.to_sql('TenQCashflow', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenQCashflow report to the DB.'
                elif report_period ==12:
                    self.checkAndAddDBColumns(report.columns,'TenKCashflow')
                    report.to_sql('TenKCashflow', con=self.dbcnx[0], if_exists='append')
                    return True, 'Successfully commited TenKCashflow report to the DB.'
                else: 
                    raise ValueError('Wrong report period of {pd} offered. Try again.'.format(pd=report_period))
            
            else: # there was a formatting error in function call
                raise ValueError("Formatting error in function call. Check your variables {rep_type} and {rep_period}".format(rep_type=report_type, rep_period=report_period))
                                
        except Exception as e:
            return False, e

    # financialHistoryExists
    # ********************** # 
    def financialHistoryExists(self, symbol, report_type, report_period):
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
            if report_type == 'is': # set the table to search
                if report_period == 3:
                    table = 'TenQIncome'
                elif report_period == 12:
                    table = 'TenKIncome'
                else: 
                    raise ValueError('Incorrect period of {rpt_pd} requested. Try again.'.format(rpt_pd = report_period)) # wrong period specified
            elif report_type == 'bs':
                if report_period == 3:
                    table = 'TenQBalance'
                elif report_period == 12:
                    table = 'TenKBalance'
                else:
                    raise ValueError('Incorrect period of {rpt_pd} requested. Try again.'.format(rpt_pd = report_period))               
            elif report_type == 'cf':
                if report_period == 3:
                    table = 'TenQCashflow'
                elif report_period == 12:
                    table = 'TenKCashflow'
                else: 
                    raise ValueError('Incorrect period {rpt_pd} requested. Try again.'.format(rpt_pd = report_period))
            else:
                raise ValueError('A report type {rpt} was requested that does not match any you offer. Try again.'.format(rpt = report_type))  # unknown report type
            # search the DB for the data
            query = 'SELECT * FROM {tbl} WHERE Symbol = ?'.format(tbl = table)
            if self.dbcnx[1].execute(query, (symbol,)).fetchone() is not None:
                return True
            else: 
                return (False, 'No records found.')
        except Exception as e:
            return (False, e)

    # priceHistoryExists
    # ****************** # 
    def priceHistoryExists(self, symbol):
        """

        Searches the DB's Price History table for the selected symbol and returns True if found.

        This function receives a string 'SYM' for the desired stock lookup. It searches the 
        database's Pricehistory table to find an instance of this symbol. If it does, the function
        returns True. Otherwise, it will return a tuple (False, 'No records msg'). 

        If the function encounters an error , it will also return a tuple (False, error message). 
        Note that any subsequent error checking built into functions that utilize this one will need
        to distinguish between a not-found False and an error False.

        Example Usage: priceHistoryExists('GOOG')

        """
        try:
            # double check to make sure this symbol is in symbol list
            if self.dbcnx[1].execute('SELECT * FROM TenYrPrices WHERE Symbol = ?', (symbol,)).fetchone() is not None:
                return True
            #otherwise return false
            return (False, 'No records found for {stock}.'.format(stock=symbol))
        except Exception as e:
            return (False, e)

    # dividendHistoryExists
    # ********************* # 
    def dividendHistoryExists(self, symbol):
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
            # double check to make sure this symbol in symbol list
            if self.dbcnx[1].execute('SELECT * FROM Dividends WHERE Symbol = ?', (symbol,)).fetchone() is not None:
                return True
            # otherwise return false
            return (False, 'No records found for {stock}.'.format(stock=symbol))
        except Exception as e:
            return (False, e)

    # checkStockFinancialsExist
    # ************************* # 
    def checkStockFinancialsExist(self, symbol):
        """

        Gatekeeper function...checks if stock already has an entry in the financial ratio's table.

        This function looks through the financial_ratios table for the symbol. The symbol argument
        is a single string of any valid ticker symbol, e.g., 'DUK'. If the symbol is found in the 
        table, the function returns a tuple with (True, 'Success message'). If fasle, a tuple with 
        (False, 'No records'). Any errors will be returned in a tuple with (False, error message).

        Example Usage: checkStockFinancialsExist('DUK')

        """

        try:
            
            if self.dbcnx[1].execute('SELECT * FROM FinancialRatios WHERE Symbol = ? LIMIT 1', (symbol,)).fetchone() is not None:
                return (True, 'The {stock} already has a record.'.format(stock=symbol))
            
            return (False, 'No records found for {stock}.'.format(stock=symbol))
        except Exception as e:
            return (False, e)

    # symbolTableExists
    # ***************** # 
    def symbolTableExists(self):
        """
        A helper function to determine whether or not the Symbol table exists in the DB. 
        If not, throw an error before any functions can try to add data to the database. 

        Returns True if a table exists, otherwise False. If error, returns tuple 
        (False, error message)

        Example Usage: symbolTableExists()
        """
        try:
            if self.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="AllStocksKey";').fetchone() is not None:
                return True
            return False 
        
        except Exception as e:
            return False, e

    # symbolExists
    # ************ #
    def symbolExists(self, symbol):
        """
        Gatekeeper. Makes sure a symbol exists before making network calls.
        Returns True if no errors. Otherwise, returns False with an error message.
        """
        try:
            # check if the symbol exists in the db
            db_symbol = self.dbcnx[1].execute('SELECT * FROM AllStocksKey WHERE Symbol = ? LIMIT 1', (symbol,)).fetchone()
            
            if db_symbol[0] != symbol: # issue a warning
                raise ValueError('The stock symbol provided, {sym}, was not found in the database. Try again.'.format(sym=symbol[0] ))    
            return True

        except Exception as e:
            return False, e
