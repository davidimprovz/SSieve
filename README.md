# SSieve Demo
Wallstreet data. A python tool for gathering, analyzing, and visualizing information on equity markets and individual equities.
**This version is posted for demo purposes for folks interested in my work and hiring me to code. You may purchase a full 10yr database of stocks from me for a flat fee of $250.**
If you're interested in licensing the full software, email me: david !at! improvz _--dot--_ com.

©2017 David Williams.
You may use this demo software package freely in your work by clearly and conspicuously acknowledging its author in the copyright of your software with the name of the author as well as this link to the original software on github.


## Overview

SSieve is a stock market data project using (mostly) PANDAS that allows its user to create a lightweight and robust stock exchange Sqlite3 database (DB).
 A lightweight DB is provided for your evaluation.

SSieve collects the latest 5-year financial history and 10-year price history of stock exchange data for all stocks listed on the NASDAQ and NYSE. Data sources are mostly made up of common info hubs. Check the globalvars.py module for more details.

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

The simplest procedure for using this code is to set up an iterator method (not provided in this code). These methods must make use of a timer so http calls are spaced over time. Use caution: *Don't slam servers with requests and get blacklisted*. Note that the timer can lengthen the data collection to take several hours (5k+ stocks x 4-10 seconds between each HTTP request).

During the software run, you should see prompts on your command line for the stock that is being retreived, or any errors that show up. After the software runs, you will see a new DB in the ./db folder that should be around 1GB in size and will have up to 12 tables. All tables except 1 use stock ticker symbols as their primary key. Additionally, you will find the results (success or failure) of all HTTP calls listed in the log file found in ./output/logs/[today's_date].log. These are immensely helpful in debugging.

The collection process can be on a stock by stock basis for testing if you are using a python session (Jupyter notebook recommended). A list of all classes and their available methods is provided below. If you want to edit the code and test from the command line as you go, you will find a simple suite of automated tests in the ./tests folder.
    
**(Database Table – Fields)** *italic tables only in full DB for $250*

1. **Symbols** - the stock symbol and the exchange it is traded on
2. **TenYrPrices** - each stock symbol paired with its most recent 10Y price history
3. **TenKIncome** - stock symbol and its most recent 5Y 10k income report
4. **TenKBalance** - stock symbol and its most recent 5Y 10k balance sheet report
5. **TenKCashflow** - stock symbol and its most recent 5Y 10k cashflow report
6. **TenQIncome** - stock symbol and its most recent 12M 10Q income report
7. **TenQBalance** - stock symbol and its most recent 12M 10Q balance sheet report
8. **TenQCashflow** - stock symbol and its most recent 12M 10Q cashflow report
9. **Dividends** - stock symbol and all available dividend payment history–inlcuding any upcoming payments–filtered to include only cash dividends
10. **financial_ratios** - stock symbol and key financial ratios associated with its performance
11. **finhealth_ratios** – stock symbol and key financial health ratios 
12. **growth_ratios** - stock symbol and key growth ratios

Three caveats for using this software:
    
1. Because this software relies on public-facing web data, it has to use http requests from urls (hence CSS / HTML structures) that could change. You will probably want to write a simple test to make sure that the urls are active before each use. If any of the urls, html / css structures have changed, just update those variables that hold the string vals.

2. The end result of running this software is a clean database of all stocks publicly traded on NASDAQ and NYSE. However, the stock market is a dynamic thing. In writing this code, the author has found issues with special characters introduced into symbols coming from the NASDAQ exchange; these special chars will at times cause confusing errors when applying transformations to data. These errors are sometimes hard to detect without visual inspection. One of the more common issues has been unicode errors. If you try to query your DB for a stock 10K/Q report that should be there and find nothing, you may need to make some corrections to the code for unforseen issues. Sometimes the companies listed on exchanges have no data, which is often true for hedgefunds and their ilk.

3. The user accepts all responsiblity and liability–financial, legal, or otherwise–for use of this software, which is not intended for use in a production environment. No warranty is offered or implied by the author. Use at your own risk. Also, treat other companies' servers with respect when programatically making http requests. That means you should probably implement the timer for retreiving data on the 5000+ stocks traded on the US's major exchanges. *Hammering servers with requests may get your IP address blacklisted and blocked*.



## Modules Overview

1. globalvars.py - contains all data source URLs 
2. dbmgt.py - handles all DB operations
3. core.py - (inherits from dbmgt) defines all the foundational methods for initializing the DB. 
4. daily.py - (inherits from core) defines all daily update methods. 

For initializing db, use `python initstocks.py [db_name].db`. 
For daily updates, use `python dailyupdate.py [./your/db/folder/path/] [db_name].db`


## Modules > Classes > Methods *free version*

### dbmgt.py - defines class stockDB()

`__init__(connection=os.getcwd() + 'trial.db')` - constructor sets DB connection string (but does not initialize the DB.) Defaults to a trial.db.

`dbcnx()` - private class property and setter method for the DB connection. At class instantiation, is a string. Once DB connection established, becomes a tuple.  

`connectToDB(connection='[default/file/path/]trial.db')` - establishes Sqlite3 connection to new DB file, or an existing file. Defaults to 'trial.db'.

`closeDBConnection(connection)` - closes the DB connection and commits all changes. Takes stockDB.dbcnx[0] as an argument.


### core.py - defines class coreStocks()

`makeStockListURL(self, exchange='NASDAQ')` - create the url to acquire the csv file containing all stocks

`getAllCurStocks(self, exchanges)` - download the csv of all stocks into a pandas dataframe

`createPriceHistoryReport(self, stock)` - clean the price history data from get10YrPriceHistory().

`get10YrPriceHistory(self, symbol)` - get price history for a stock

`getDividendHistory(self, symbol, period)` - acquire the dividend history for the given symbol and for the period specified

`getStockFinancials(self, symbol)` - gatekeeper method...check if a stock already has an entry in the financial ratio's table

`get10KQReport(self, symbol, report_type, freq)` - get the 10K or 10Q reports for any given stock and for the specified frequency (e.g., 5yr, 5mon)

`format10KQSheet(self, sheet, symbol, report)` - Helper method...using get10KQReport, format the financial data

`create10KCashflowReport(self, symbol)` - clean and package the 10K cashflow report for the given symbol into a pandas dataframe

`create10QCashflowReport(self, symbol)` - clean and package the 10Q cashflow report for the given symbol into a pandas dataframe

`createSymbolsKeyTable(self, symbols)` - create the ticker symbol table 'Symbol' in DB

`commitPriceHistory(self, data, daily=False)` - send price history to the DB

`commitFinancialsData(self, report, report_type, report_period)` - send the stock's financial ratio data to the database

`financialHistoryExists(self, symbol, report_type, report_period)` - check to see if the table with the financial history for the given stock is already present in the DB

`priceHistoryExists(self, symbol)` - check the DB to see if the given stock's price history table is already available

`symbolTableExists(self)` - determine whether or not the Symbol table exists in the DB

`symbolExists(self, symbol)` - ensure symbol exists before making network calls to acquire data

`alignReportColumns(sheet)` - align column names to allow for setting the index of the sheet as stock symbol

`cleanNullColumns(sheet)` - discard columns in sheets where each row in column is null

`removeColumnSpaces(sheet)` - format a column name to remove spaces


### daily.py - defines class dailyStocks()

`dailyTimeDelayPriceUpdate(stocks)` - selects all stock tickers from DB and iterates over them, updating price history with calls to getRecentMngStarPriceInfo()

`renameStocks(changed_tickers)` - Accepts dataframe from checkStockNameChanges() and processes the changes in the DB switching tickers in each table

`updateAllStocksTable(new_stocks)` - Checks the DB Symbol table for new stocks in the exchanges and adds these stocks to the DB Symbol table

`getRecentMngStarPriceInfo(stock)` -  acquire all recent (what's currently missing) pricing data for a stock already in the DB

`compareStockListsWithIsIn(db_list, new_list)` - Compare DB Symbol list with current NASDAQ list to separate old stocks from new stocks
