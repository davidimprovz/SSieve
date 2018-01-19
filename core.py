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
            else: 
                raise ValueError( 'Variable with incorrect data type passed. A dataframe is required but a {kind} was passed'.format(kind=type(sheet)) )
        except Exception as e:
            return False, e

        return sheet

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
            all_exchanges = all_exchanges[ (all_exchanges.loc[:, 'LastSale'] != None) ] # (all_exchanges.loc[:,'LastSale'] != 'n/a') & 
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
