# SSieve
Python tools for gathering, analyzing, and visualizing information on equity markets and individual equities.

WallstreetDB.py: Wallstreet Data Acquisition
©2017 David Williams. Creative Commons License applies.
You may use this software freely in your work by clearly and conspicuously acknowledging its author in the copyright of your software with the name of the author as well as this link to the original software on github.


## Overview

WallstreetDB.py is a stockmarket data project using (mostly) PANDAS that allows its user to create a lightweight and robust stock exchange DB. Think of this as a starter pack that you can expand on. It (mostly) collects the latest 5-year history of stock exchange data for all stocks listed on the NASDAQ and NYSE. (Check the source code for data sources, of which there could be several, depending on your preference.)

The resulting database serves as a convenient reference for testing algorithms on one's own system without the hassle or vulnerability of 3rd party signups and poorly-maintained APIs.

The workflow in this module is as follows:

1. Initialize your project with a database path.
2. Retreive a list of all NYSE- and NASDAQ-listed stocks form the NASDAQ's comprehensive list. This list includes symbols, most recent price, company name, etc.
3. Clean and commit the acquired data to an SQLite3 database, which you name when you initialize in step 1.
4. Collect desired stock and market data to include: 
    * 10K reports: Income, Balance, Cashflow
    * 10Q reports: Income, Balance, Cashflow
    * (up to) 10Y Stock Price History
    * Dividend payments, if applicable
    * Key ratios and miscellaneous financial details for each stock.

The collection process can be on a stock by stock basis or you can iterate over all of the stock symbols in your DB. 

A list of all available functions is provided. The simple procedure is to iterate with a timer that spaces out http calls to servers so you *don't slam those servers with requests and get blacklisted*. A convenient function `timeDelayDataPopulate()` is provided for this task and has been successfully tested. Use a variable to capture the success / failure messages of `timeDelayDataPopulate()`. 

Note that due to the timer, this call can take several hours to completely download all available stock data. The generated database (about 1GB) will have up to 12 tables, all of which use stock ticker symbols as their primary key.
    
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

Three caveats to for using this module:
    
1. Because this software relies on public-facing web data, it has to use http requests from urls (hence CSS / HTML structures) that could change. You will probably want to write a simple test to make sure that the urls are active before each use. If any of the urls, html / css structures have changed, just update those variables that hold the string vals.

2. The end result of running this software is a (mostly) clean database of all stocks publicly traded on NASDAQ and NYSE. However, the stock market is a dynamic thing. In writing this code, the author has found issues with special characters introduced into symbols coming from the NASDAQ exchange; these special chars will at times cause confusing errors which are hard to detect without visual inspection. One of the more common issues has been unicode errors. Also, if you try to query your DB for a stock 10K/Q report that should be there and find nothing, you may need to make some corrections to the code for unforseen issues.

3. The user accepts all responsiblity and liability for use of this software, which is not intended for use in a production environment and has not been fully tested. No warranty is offered or implied by the author. Use at your own risk. Also, treat other companies' servers with respect when programatically making http requests. That means you should probably implement the timer for iteratively retreiving data on the 5000+ stocks traded on the US's major exchanges. *Hammering servers with requests may get your IP address blacklisted and blocked*.


## Function List: Name() - Description


### Helpers to Automate 

`populateAllFinancialReportsForStock(symbol)` - package handler call that takes the given symbol and fires off all previous function calls pertaining to data acquisition

`timeDelayDataPopulate()` - timer implementation to handle iterative http requests to servers for data


### Database

`setDBPath(db_path)` - set your DB file path..not currently implemented.

`makeStockListURL(exchange)` - create the url to acquire the csv file containing all stocks

`getAllCurStocks(exchanges)` - download the csv of all stocks into a pandas dataframe

`createSymbolsKeyTable(symbols)` - create the Symbol table 

`symbolTableExists()` - check to see if the table, Symbol, exists

`dropAllTables()` - helper function to clear the db if you want to make a clean copy

`closeDBConnection()` - call this function when finished acquiring data...automatically invoked if you're using the helper method timeDelayDataPopulate()


### Price History

`get10YrPriceHistory(symbol)` - get the price history for any given stock symbol

`createPriceHistoryReport(symbol)` - caller function for get10YrPriceHistory(). Cleans the price history.

`commitPriceHistory(data)` - send the price history to the DB

`priceHistoryExists(symbol)` - check the DB to see if the given stock's price history table is present in the DB

`updateStockPrices(symbol)` - daily update function to collect the most recent prices (beginning, ending, daily high and low)


### TenK & TenQ Reports

`get10KQReport(symbol, report_type, freq)` - get the 10K or 10Q reports for any given stock and for the specified frequency (e.g., 5yr, 5mon)

`create10KIncomeReport(symbol)` - clean and package the 10K income report for the given symbol into a pandas dataframe

`create10KBalanceReport(symbol)` – clean and package the 10K balance sheet report for the given symbol into a pandas dataframe

`create10KCashflowReport(symbol)` – clean and package the 10K cashflow report for the given symbol into a pandas dataframe

`create10QIncomeReport(symbol)` – clean and package the 10Q income report for the given symbol into a pandas dataframe

`create10QBalanceReport(symbol)` – clean and package the 10Q balance sheet report for the given symbol into a pandas dataframe

`create10QCashflowReport(symbol)` – clean and package the 10Q cashflow report for the given symbol into a pandas dataframe

`commitFinancialsData(report, report_type, report_period)` - send the acquired financial data to the database


### Dividends

`dividendHistoryExists(symbol)` - check if the dividend table exists in the database, and whether the given symbol's dividend history has already been downloaded 

`getDividendHistory(symbol, period)` - acquire the dividend history for the given symbol and for the period specified

`formatRawDivTable(soup, which)` - clean the raw dividend history and package it with a pandas dataframe

`commitDividendHistory(data)` - save the dividend history to the database


### Financial Ratios

`financialHistoryExists(symbol, report_type, report_period)` - check to see if the table with the financial history for the given stock is already present in the DB

`createStockFinancialsReports(symbol)` - 

`checkStockFinancialsExist(symbol)` - 

`getStockFinancials(symbol)` - 

`commitStockFinancials(financial_reports)` –
