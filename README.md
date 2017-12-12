# SSieve
Wallstreet data. A python tool for gathering, analyzing, and visualizing information on equity markets and individual equities.

©2017 David Williams. Creative Commons License applies.
You may use this software freely in your work by clearly and conspicuously acknowledging its author in the copyright of your software with the name of the author as well as this link to the original software on github.


## Overview

WallstreetDB.py is a stock market data project using (mostly) PANDAS that allows its user to create a lightweight and robust stock exchange Sqlite3 database (DB). Think of this as a starter pack that you can expand on. It (mostly) collects the latest 5-year history of stock exchange data for all stocks listed on the NASDAQ and NYSE. Data sources are mostly made up of common info hubs. Check the globalvars.py module for more details. Since this software is expected to change over time, you can expect more sources.

The resulting DB serves as a reference for testing algorithms on one's own system without the hassle or vulnerability of 3rd party services or poorly-maintained APIs.

The workflow in this module, which is coded in initstocks.py and dailyupdate.py, is as follows:

1. Supply a database path and start a new Sqlite3 DB.
2. Retreive a list of all NYSE- and NASDAQ-listed stocks using the NASDAQ's comprehensive list. This list includes symbols, most recent price, company name, etc.
3. Clean and commit the acquired data from step 2.
4. For each stock, collect its data: 
    * 10K reports: Income, Balance, Cashflow
    * 10Q reports: Income, Balance, Cashflow
    * (up to) 10Y Stock Price History
    * Dividend payments, if applicable
    * Key ratios and miscellaneous financial details.
5. Daily update of all stocks in DB, as well as adding stocks new to the markets.

The simplest procedure for using this code is to call the iterator methods provided. These methods make use of a timer so http calls are spaced over time. Use caution when editing the timer; *don't slam those servers with requests and get blacklisted*. One of these iterative methods is `timeDelayDataPopulate()`. Note that the timer can lengthen the time this method runs to take several hours (5k+ stocks x 4-10 seconds between each HTTP request). 

During the software run, you should see prompts on your command line for the stock that is being retreived, or any errors that show up. After the software runs, you will see a new DB in the ./db folder that should be around 1GB in size and will have up to 12 tables. All tables except 1 use stock ticker symbols as their primary key. Additionally, you will find the results (success or failure) of all HTTP calls listed in the log file found in ./output/logs/[today's_date].log. These are immensely helpful in debugging.

The collection process can be on a stock by stock basis for testing if you are using a python session (Jupyter notebook recommended). A list of all classes and their available methods is provided below. If you want to edit the code and test from the command line as you go, you will find a simple suite of automated tests in the ./tests folder.
    
**(Database Table – Fields)**

1. **Symbols** - the stock symbol and the exchange it is traded on
2. **Ten_Yr_Prices** - each stock symbol paired with its most recent 10Y price history
3. **Ten_K_Income** - stock symbol and its most recent 5Y 10k income report
4. **Ten_K_Balance** - stock symbol and its most recent 5Y 10k balance sheet report
5. **Ten_K_Cashflow** - stock symbol and its most recent 5Y 10k cashflow report
6. **Ten_Q_Income** - stock symbol and its most recent 12M 10Q income report
7. **Ten_Q_Balance** - stock symbol and its most recent 12M 10Q balance sheet report
8. **Ten_Q_Cashflow** - stock symbol and its most recent 12M 10Q cashflow report
9. **Dividends** - stock symbol and all available dividend payment history–inlcuding any upcoming payments–filtered to include only cash dividends
10. **financial_ratios** - stock symbol and key financial ratios associated with its performance
11. **finhealth_ratios** – stock symbol and key financial health ratios 
12. **growth_ratios** - stock symbol and key growth ratios

Three caveats to for using this software:
    
1. Because this software relies on public-facing web data, it has to use http requests from urls (hence CSS / HTML structures) that could change. You will probably want to write a simple test to make sure that the urls are active before each use. If any of the urls, html / css structures have changed, just update those variables that hold the string vals.

2. The end result of running this software is a clean database of all stocks publicly traded on NASDAQ and NYSE. However, the stock market is a dynamic thing. In writing this code, the author has found issues with special characters introduced into symbols coming from the NASDAQ exchange; these special chars will at times cause confusing errors which are hard to detect without visual inspection. One of the more common issues has been unicode errors. Also, if you try to query your DB for a stock 10K/Q report that should be there and find nothing, you may need to make some corrections to the code for unforseen issues. Sometimes the companies listed on exchanges have no data, which is often true for hedgefunds and their ilk.

3. The user accepts all responsiblity and liability–financial, legal, or otherwise–for use of this software, which is not intended for use in a production environment and has not been fully tested. No warranty is offered or implied by the author. Use at your own risk. Also, treat other companies' servers with respect when programatically making http requests. That means you should probably implement the timer for retreiving data on the 5000+ stocks traded on the US's major exchanges. *Hammering servers with requests may get your IP address blacklisted and blocked*.



## Modules Overview

1. globalvars.py - contains all data source URLs 
2. dbmgt.py - handles all DB operations
3. core.py - (inherits from dbmgt) defines all the foundational methods for initializing the DB. 
4. daily.py - (inherits from core) defines all daily update methods. 

For initializing db, use `python initstocks.py [db_name].db`. 
For daily updates, use `python dailyupdate.py [./your/db/folder/path/] [db_name].db`


## Modules > Classes > Methods

### dbmgt.py - defines class stockDB()

`__init__(connection=os.getcwd() + 'trial.db')` - sets DB connection string (but does not initialize the DB.) Defaults to a trial.db.
`dbcnx()` - private class property and setter method for the DB connection. At class instantiation, is a string. Once DB connection established, becomes a tuple.  
`connectToDB(connection='[default/file/path/]trial.db')` - establishes Sqlite3 connection to new DB file, or an existing file. Defaults to 'trial.db'.
`closeDBConnection(connection)` - closes the DB connection and commits all changes. Takes stockDB.dbcnx[0] as an argument.
`dropAllTables()` - drops all tables for the current DB. Used only for testing purposes. Recommend commenting out this method before deploying code.
`checkAndAddDBColumns(dataframe_columns, table)` - compares columns in incoming dataframe with those in the DB table name passed as second argument. If new columns exist, they are added to the table before any calls to commit data. 
`testDBTables(cursor)` - Checks the DB to see which tables exist. Accepts a cursor argument, stockDB.dbcnx[1], and returns a tuple of table count with the names of tables.

### core.py - defines class coreStocks()

`timeDelayDataPopulate(self, stocks)` - timer implementation to handle iterative http requests to servers for data
`populateAllFinancialReportsForStock(self, stock, daily=False)` - package handler call that takes the given symbol and fires off all previous function calls pertaining to data acquisition
`makeStockListURL(self, exchange='NASDAQ')` - create the url to acquire the csv file containing all stocks
`getAllCurStocks(self, exchanges)` - download the csv of all stocks into a pandas dataframe
`createPriceHistoryReport(self, stock)` - clean the price history data from get10YrPriceHistory().
`get10YrPriceHistory(self, symbol)` - get price history for a stock
`getDividendHistory(self, symbol, period)` - acquire the dividend history for the given symbol and for the period specified
`formatRawDivTable(soup, which)` - clean the raw dividend history and package it with a pandas dataframe
`getStockFinancials(self, symbol)` - gatekeeper method...check if a stock already has an entry in the financial ratio's table
`createStockFinancialsReports(self, symbol)` - gather and format the financial ratios from getStockFinancials() into 3 separate dataframes
`get10KQReport(self, symbol, report_type, freq)` - get the 10K or 10Q reports for any given stock and for the specified frequency (e.g., 5yr, 5mon)
`format10KQSheet(self, sheet, symbol, report)` - Helper method...using get10KQReport, format the financial data
`create10KIncomeReport(self, symbol)` - clean and package the 10K income report for the given symbol into a pandas dataframe
`create10KBalanceReport(self, symbol)` - clean and package the 10K balance sheet report for the given symbol into a pandas dataframe
`create10KCashflowReport(self, symbol)` - clean and package the 10K cashflow report for the given symbol into a pandas dataframe
`create10QIncomeReport(self, symbol)` - clean and package the 10Q income report for the given symbol into a pandas dataframe
`create10QBalanceReport(self, symbol)` - clean and package the 10Q balance sheet report for the given symbol into a pandas dataframe
`create10QCashflowReport(self, symbol)` - clean and package the 10Q cashflow report for the given symbol into a pandas dataframe
`createSymbolsKeyTable(self, symbols)` - create the ticker symbol table 'Symbol' in DB
`commitPriceHistory(self, data, daily=False)` - send price history to the DB
`commitDividendHistory(self, data, monthly=False)` - save the dividend history to the database
`commitStockFinancials(self, financial_reports)` - save the 3 financial history dataframes to their respective tables in DB. 
`commitFinancialsData(self, report, report_type, report_period)` - send the stock's financial ratio data to the database
`financialHistoryExists(self, symbol, report_type, report_period)` - check to see if the table with the financial history for the given stock is already present in the DB
`priceHistoryExists(self, symbol)` - check the DB to see if the given stock's price history table is already available
`dividendHistoryExists(self, symbol)` - check if the dividend table exists in the database, and whether the given symbol's dividend history has already been downloaded 
`checkStockFinancialsExist(self, symbol)` - gatekeeper function...check if stock already has an entry in the financial ratio's table
`symbolTableExists(self)` - determine whether or not the Symbol table exists in the DB
`symbolExists(self, symbol)` - ensure symbol exists before making network calls to acquire data
`alignReportColumns(sheet)` - align column names to allow for setting the index of the sheet as stock symbol
`cleanNullColumns(sheet)` - discard columns in sheets where each row in column is null
`removeColumnSpaces(sheet)` - format a column name to remove spaces

### daily.py - defines class dailyStocks()

`dailyTimeDelayPriceUpdate(stocks)` - selects all stock tickers from DB and iterates over them, updating price history with calls to getRecentMngStarPriceInfo()
`checkStockNameChanges()` - collects dataframe from NASDAQ of all stocks that have changed ticker symbols
`renameStocks(changed_tickers)` - Accepts dataframe from checkStockNameChanges() and processes the changes in the DB switching tickers in each table
`updateAllStocksTable(new_stocks)` - Checks the DB Symbol table for new stocks in the exchanges and adds these stocks to the DB Symbol table
`getRecentMngStarPriceInfo(stock)` -  acquire all recent (what's currently missing) pricing data for a stock already in the DB
`compareStockListsWithIsIn(db_list, new_list)` - Compare DB Symbol list with current NASDAQ list to separate old stocks from new stocks
