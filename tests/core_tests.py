# core_tests.py

# Exercises the methods of the coreStocks class
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

import core


# todo: for clairty, make coreClassTests a multithreaded class with methods 
# that print their results and invoke from main function.

def coreClassTests():
	"""
	Exercises the core stock scraping daemon module consisting
	of the coreStocks class and several helper methods.
	"""
	try:

		# variables
		test_db = os.getcwd() + '/db/core_function_tests.db'
		exchanges = ('NASDAQ', 'NYSE')
		nyse_symbol = ('DUK','NYSE')
		nasdaq_symbol = ('MSFT', 'NASDAQ')

		# instantiate coreStocks class, connect to db, and test
		new_stocks = core.coreStocks(test_db)
		new_stocks.connectToDB(new_stocks.dbcnx)
		assert isinstance(new_stocks, core.coreStocks), "failed to instantiate coreStocks"
		assert isinstance(new_stocks.dbcnx[0], sqlite3.Connection), "failed to connect to DB with message: %r" % new_stocks.dbcnx[0]
		# print db path for reference
		print('\n' + new_stocks.dbcnx[2])
		
		
		# BEGIN exercise of coreStocks

		# test stock list html path
			# todo: test for valid html address
		assert new_stocks.makeStockListURL('NASDAQ') == 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download', "incorrect string for NASDAQ url: %r" % new_stocks.makeStockListURL('NASDAQ')
		assert new_stocks.makeStockListURL('NYSE') == 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download', "incorrect string for NYSE url: %r" % new_stocks.makeStockListURL('NYSE')


		# test stock list instance type and contents
		stock_list = new_stocks.getAllCurStocks(exchanges)
		sl_expected_cols = ['Symbol', 'Name', 'LastSale', 'MarketCap', 'IPOyear', 'Sector', 'Industry', 'Market', 'MarketCapSym']
		assert isinstance(stock_list, pd.DataFrame), "NASDAQ stock list is not a dataframe: returned %r" % type(stock_list)
		assert stock_list.index.size > 1, "NASDAQ stock list was empty."
		assert len(stock_list.columns) == len(sl_expected_cols), "NASDAQ stock list missing columns: got %r" % len(stock_list.columns)
		assert all(i in stock_list.columns for i in sl_expected_cols), "NASDAQ stock list columns don't match: got %r" % stock_list.columns


		# test raw price history report
		price_report = new_stocks.get10YrPriceHistory(nyse_symbol)
		pr_expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
		assert isinstance(price_report, pd.DataFrame), "Raw price history report not a dataframe: returned %r" % type(price_report)
		assert price_report.index.size > 1,  "Raw price history report was empty."
		assert len(price_report.columns) == len(pr_expected_cols), "Raw price history report missing columns: got %r" % len(price_report.columns)
		assert all(i in price_report.columns for i in pr_expected_cols), "Raw price history report columns don't match: got %r" % price_report.columns


		# test creation of final price history report
		pr_hist_report = new_stocks.createPriceHistoryReport(nyse_symbol)
		pr_hist_cols = ['Reference', 'Open', 'High', 'Low', 'Close', 'Volume']
		assert isinstance(pr_hist_report, pd.DataFrame), "Formatted price history report not a dataframe: returned %r" % type(pr_hist_report)
		assert pr_hist_report.index.size > 1, "Formatted price history report was empty."
		assert len(pr_hist_report.columns) == len(pr_hist_cols), "Formatted price history report missing columns: got %r" % len(pr_hist_report.columns)
		assert all(i in pr_hist_report.columns for i in pr_hist_cols), "Formatted price history report columns don't match: got %r" % pr_hist_report.columns

		
		# test dividend history
		div_hist = new_stocks.getDividendHistory(nasdaq_symbol, 10)
		div_hist_cols = ['Ex-Dividend_Date', 'Declaration_Date', 'Record_Date', 'Payable_Date','Dividend_Type', 'Amount', 'Currency']
		assert isinstance(div_hist, pd.DataFrame), "Dividend report not a dataframe: returned %r" % type(div_hist)
		assert div_hist.index.size > 1, "Dividend report was empty."
		assert len(div_hist.columns) == len(div_hist_cols), "Dividend report missing columns: got %r" % len(div_hist.columns)
		assert all(i in div_hist.columns for i in div_hist_cols), "Dividend report columns don't match: got %r" % div_hist.columns


		# test raw stock financial reports
		fin_report = new_stocks.getStockFinancials(nyse_symbol)
		assert isinstance(fin_report, pd.DataFrame), "Raw financials report not a dataframe: returned %r" % type(fin_report)
		assert fin_report.index.size > 1, "Raw financials report was empty."
        # assert len(fin_report.columns) == len(fin_report_cols)
        # assert all(i in fin_report.columns for i in fin_report_cols)


		# test finished financial report 
		financials = new_stocks.createStockFinancialsReports(nasdaq_symbol)
		assert isinstance(financials, tuple), "Formatted financials report not a tuple: returned %r" % type(financials)
		assert all(isinstance(i, pd.DataFrame) for i in financials), "Formatted financials reports not dataframes: returned %r" % [type(i) for i in financials]
		assert all(i.index.size > 1 for i in financials), "One or more formatted financial reports were empty: %r." % [i.index.size for i in financials]

		# test raw 10K/Q reports
		raw_tk_inc = new_stocks.get10KQReport(nasdaq_symbol, 'is', 12)
		assert isinstance(raw_tk_inc, pd.DataFrame), "Raw 10k inc report not a dataframe: returned %r" % type(raw_tk_inc)
		assert raw_tk_inc.index.size > 1, "Raw 10k inc report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)
			
		raw_tk_bs = new_stocks.get10KQReport(nasdaq_symbol, 'bs', 12)
		assert isinstance(raw_tk_bs, pd.DataFrame), "Raw 10k bs report not a dataframe: returned %r" % type(raw_tk_bs)
		assert raw_tk_bs.index.size > 1, "Raw 10k bs report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)

		raw_tk_cf = new_stocks.get10KQReport(nasdaq_symbol, 'cf', 12)
		assert isinstance(raw_tk_cf, pd.DataFrame), "Raw 10k cf report not a dataframe: returned %r" % type(raw_tk_cf)
		assert raw_tk_cf.index.size > 1, "Raw 10k cf report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)

		raw_tq_inc = new_stocks.get10KQReport(nasdaq_symbol, 'is', 3)
		assert isinstance(raw_tq_inc, pd.DataFrame), "Raw 10q inc report not a dataframe: returned %r" % type(raw_tq_inc)
		assert raw_tq_inc.index.size > 1, "Raw 10q inc report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)

		raw_tq_bs = new_stocks.get10KQReport(nasdaq_symbol, 'bs', 3)
		assert isinstance(raw_tq_bs, pd.DataFrame), "Raw 10q bs report not a dataframe: returned %r" % type(raw_tq_bs)
		assert raw_tq_bs.index.size > 1, "Raw 10q bs report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)

		raw_tq_cf = new_stocks.get10KQReport(nasdaq_symbol, 'cf', 3)
		assert isinstance(raw_tq_cf, pd.DataFrame), "Raw 10q cf report not a dataframe: returned %r" % type(raw_tq_cf)
		assert raw_tq_cf.index.size > 1, "Raw 10q cf report was empty."
        # assert len(div_hist.columns) == len(div_hist_cols)
        # assert all(i in div_hist.columns for i in div_hist_cols)


		# test finished 10K income statement 
		k_inc_report = new_stocks.create10KIncomeReport(nyse_symbol)
		assert isinstance(k_inc_report, pd.DataFrame), "Formatted 10k inc report not a dataframe: returned %r" % type(k_inc_report)
		assert k_inc_report.index.size > 1, "Formatted 10k inc report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test finished 10K balance sheet
		k_bs_report = new_stocks.create10KBalanceReport(nyse_symbol)
		assert isinstance(k_bs_report, pd.DataFrame), "Formatted 10k bs report not a dataframe: returned %r" % type(k_bs_report)
		assert k_bs_report.index.size > 1, "Formatted 10k bs report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test finished 10K cashflow sheet 
		k_cf_report = new_stocks.create10KCashflowReport(nyse_symbol)
		assert isinstance(k_cf_report, pd.DataFrame), "Formatted 10k cf report not a dataframe: returned %r" % type(k_cf_report)
		assert k_cf_report.index.size > 1, "Formatted 10k cf report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test finished 10Q income sheet 
		q_inc_report = new_stocks.create10QIncomeReport(nyse_symbol)
		assert isinstance(q_inc_report, pd.DataFrame), "Formatted 10q inc report not a dataframe: returned %r" % type(q_inc_report)
		assert q_inc_report.index.size > 1, "Formatted 10q inc report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test finished 10q balance sheet 
		q_bs_report = new_stocks.create10QBalanceReport(nyse_symbol)
		assert isinstance(q_bs_report, pd.DataFrame), "Formatted 10q bs report not a dataframe: returned %r" % type(q_bs_report)
		assert q_bs_report.index.size > 1, "Formatted 10q bs report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test finished 10q cashflow sheet 
		q_cf_report = new_stocks.create10QCashflowReport(nyse_symbol)
		assert isinstance(q_cf_report, pd.DataFrame), "Formatted 10q cf report not a dataframe: returned %r" % type(q_cf_report)
		assert q_cf_report.index.size > 1, "Formatted 10q cf report was empty."
        # assert len(k_inc_report.columns) == len(k_inc_cols)
        # assert all(i in k_inc_report.columns for i in k_inc_cols)


		# test commit of price history and query DB for the table 
		pr_hist_commit = new_stocks.commitPriceHistory(pr_hist_report)
		assert pr_hist_commit[0] is True, "Price history commit failed."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenYrPrices";').fetchone() is not None, "Ten year price history DB commit failed. No table called TenYrPrices."
		test_retrieve =  pd.read_sql('SELECT * FROM TenYrPrices WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		assert isinstance(test_retrieve, pd.DataFrame), "Price history from DB not a dataframe: returned %r" % type(test_retrieve)
		assert test_retrieve.index.size > 1, "Price history from DB is empty."


		# test commit of dividend history
		div_hist_commit = new_stocks.commitDividendHistory(div_hist)
		assert div_hist_commit[0] is True, "Failed to commit dividend history to DB."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="Dividends";').fetchone() is not None, "Failed to create Dividends table in DB."
		test_retrieve =  pd.read_sql('SELECT * FROM Dividends WHERE Symbol = "{item}";'.format(item=nasdaq_symbol[0]), con=new_stocks.dbcnx[0])
		assert isinstance(test_retrieve, pd.DataFrame), "The dividend report from DB was not a pandas dataframe. Got %r instead." % type(test_retrieve)
		assert test_retrieve.index.size > 1, "The dividend report from DB was empty."


		# test commits of stock financial reports
		fin_ratio_commit = new_stocks.commitStockFinancials(financials)
		assert fin_ratio_commit[0] is True, "Failed to commit financial ratios report to DB."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="FinancialRatios";').fetchone() is not None, "Failed to create FinancialRatios table in DB."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="FinHealthRatios";').fetchone() is not None, "Failed to create FinHealthRatios table in DB."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="GrowthRatios";').fetchone() is not None, "Failed to create GrowthRatios table in DB."
		financials_retrieve = pd.read_sql('SELECT * FROM FinancialRatios WHERE Symbol = "{item}";'.format(item=nasdaq_symbol[0]), con=new_stocks.dbcnx[0])
		finhealth_retrieve = pd.read_sql('SELECT * FROM FinHealthRatios WHERE Symbol = "{item}";'.format(item=nasdaq_symbol[0]), con=new_stocks.dbcnx[0])
		growth_retrieve = pd.read_sql('SELECT * FROM GrowthRatios WHERE Symbol = "{item}";'.format(item=nasdaq_symbol[0]), con=new_stocks.dbcnx[0])
		assert isinstance(financials_retrieve, pd.DataFrame), "The Financial ratios report from DB was not a pandas dataframe. Got %r instead." % type(financials_retrieve)
		assert isinstance(finhealth_retrieve, pd.DataFrame), "The Financial health ratios report from DB was not a pandas dataframe. Got %r instead." % type(finhealth_retrieve)
		assert isinstance(growth_retrieve, pd.DataFrame), "The Growth ratios report from DB was not a pandas dataframe. Got %r instead." % type(growth_retrieve)
		assert financials_retrieve.index.size > 1, "Financial ratios dataframe from DB is empty."
		assert finhealth_retrieve.index.size > 1, "Finhealth dataframe from DB is empty." 
		assert growth_retrieve.index.size > 1, "Growth ratios dataframe from DB is empty."


		# test commits of financial data
		k_is_commit = new_stocks.commitFinancialsData(k_inc_report, 'is', 12)
		k_bs_commit = new_stocks.commitFinancialsData(k_bs_report, 'bs', 12)
		k_cf_commit = new_stocks.commitFinancialsData(k_cf_report, 'cf', 12)
		q_is_commit = new_stocks.commitFinancialsData(q_inc_report, 'is', 3)
		q_bs_commit = new_stocks.commitFinancialsData(q_bs_report, 'bs', 3)
		q_cf_commit = new_stocks.commitFinancialsData(q_cf_report, 'cf', 3)

		assert k_is_commit[0] is True, "Failed to commit 10k inc report to DB."
		assert k_bs_commit[0] is True, "Failed to commit 10k bs report to DB."
		assert k_cf_commit[0] is True, "Failed to commit 10k cf report to DB."
		assert q_is_commit[0] is True, "Failed to commit 10q inc report to DB."
		assert q_bs_commit[0] is True, "Failed to commit 10q bs report to DB."
		assert q_cf_commit[0] is True, "Failed to commit 10q cf report to DB."

		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenKIncome";').fetchone() is not None, "Ten k inc DB commit failed. No table called TenKIncome."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenKBalance";').fetchone() is not None, "Ten k bs DB commit failed. No table called TenKBalance."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenKCashflow";').fetchone() is not None, "Ten k cf DB commit failed. No table called TenKCashflow."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenQIncome";').fetchone() is not None, "Ten q inc DB commit failed. No table called TenQIncome."
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenQBalance";').fetchone() is not None, "Ten q bs DB commit failed. No table called TenQBalance"
		assert new_stocks.dbcnx[1].execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name="TenQCashflow";').fetchone() is not None, "Ten q bs DB commit failed. No table called TenQCashflow"

		k_inc_retrieve = pd.read_sql('SELECT * FROM TenKIncome WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		k_bs_retrieve = pd.read_sql('SELECT * FROM TenKBalance WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		k_cf_retrieve = pd.read_sql('SELECT * FROM TenKCashflow WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		q_inc_retrieve = pd.read_sql('SELECT * FROM TenQIncome WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		q_bs_retrieve = pd.read_sql('SELECT * FROM TenQBalance WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		q_cf_retrieve = pd.read_sql('SELECT * FROM TenQCashflow WHERE Symbol = "{item}";'.format(item=nyse_symbol[0]), con=new_stocks.dbcnx[0])
		
		assert isinstance(k_inc_retrieve, pd.DataFrame), "Ten K inc from DB not a dataframe: returned %r" % type(k_inc_retrieve)
		assert isinstance(k_bs_retrieve, pd.DataFrame), "Ten K bs from DB not a dataframe: returned %r" % type(k_bs_retrieve)
		assert isinstance(k_cf_retrieve, pd.DataFrame), "Ten K cf from DB not a dataframe: returned %r" % type(k_cf_retrieve)
		assert isinstance(q_inc_retrieve, pd.DataFrame), "Ten Q inc from DB not a dataframe: returned %r" % type(q_inc_retrieve)
		assert isinstance(q_bs_retrieve, pd.DataFrame), "Ten Q bs from DB not a dataframe: returned %r" % type(q_bs_retrieve)
		assert isinstance(q_cf_retrieve, pd.DataFrame), "Ten Q inc from DB not a dataframe: returned %r" % type(q_cf_retrieve)

		assert k_inc_retrieve.index.size > 1, "Ten K inc from DB is empty." 
		assert k_bs_retrieve.index.size > 1, "Ten K bs from DB is empty." 
		assert k_cf_retrieve.index.size > 1, "Ten K cf from DB is empty." 
		assert q_inc_retrieve.index.size > 1, "Ten Q inc from DB is empty." 
		assert q_bs_retrieve.index.size > 1, "Ten Q bs from DB is empty." 
		assert q_cf_retrieve.index.size > 1, "Ten Q cf from DB is empty." 


		# test populateAllFinancialReportsForStock for a stock picked at random
		stock_list = stock_list[['Symbol', 'Market']]
		new_stocks.dropAllTables() # clear the DB
		symbol_table = new_stocks.createSymbolsKeyTable(stock_list) # test create symbol key table
		assert symbol_table is True, "Failed to create the symbols key table. Returned %r" % symbol_table[1]

		rand_num = np.random.randint(0, stock_list.index.size)
		single_stock = stock_list.iloc[rand_num]
		results = new_stocks.populateAllFinancialReportsForStock(single_stock)


		# test timeDelayDataPopulate of a series of stocks picked at random
		new_stocks.dropAllTables() # clear the DB
		new_stocks.createSymbolsKeyTable(stock_list)
		stock_series = stock_list.sample(2) 
		results = new_stocks.timeDelayDataPopulate(stock_series)


		# test tear down of db connection 
		print('\n' + str(new_stocks.closeDBConnection(new_stocks.dbcnx[0])))


		# delete db
		os.remove(test_db)

		return 'Tests completed. No errors to report.'

	except Exception as e:
		return ('\n!!!! A problem occured !!!!\n', e, '\n')

if __name__ == '__main__':
	message = coreClassTests()

	if isinstance(message, tuple):
		for i in message:
			print (i)
	else: print('\n' + message + '\n')
	
