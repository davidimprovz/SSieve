# dbmgt_tests.py

# Exercises the methods of the stockDB class
# each time a change is made to the code base.

import os, sys, sqlite3
import pandas as pd

sys.path.append('../')
import dbmgt
import core


def stockDBClassTests():
	"""
	Exercises the stockDB methods for managing sqlite3
	DB files used to store stock information.
	"""
	try:
		# variables
		test_db_1 = os.getcwd() + '/db/db_tests_1.db'
		test_db_2 = os.getcwd() + '/db/db_tests_2.db'
		nyse_symbol = ('MMM', 'NYSE')
		stock_test = core.coreStocks(test_db_2) # instantiate core class
		pr_hist_report = stock_test.createPriceHistoryReport(nyse_symbol) # a dataframe of stock info for testing

		
		# make a DB folder in this directory if doesn't already exist


		# print stock_test db path for reference
		stock_test.connectToDB(stock_test.dbcnx)
		print('\n' + stock_test.dbcnx[2])


		# instantiate a stockDB class, connect to it, and test it
		stockdb = dbmgt.stockDB(test_db_1)
		stockdb.connectToDB(stockdb.dbcnx)
		assert isinstance(stockdb, dbmgt.stockDB), "Failed to instantiate stockDB instance: got %r instead." % type(stockdb)
		assert isinstance(stockdb.dbcnx[0], sqlite3.Connection), "Failed to connect to a database. Got %r instead." % stockdb.dbcnx[1]
		# close this connnection and remove the first db
		stockdb.closeDBConnection(stockdb.dbcnx[0])
		os.remove(test_db_1)


		# test checkAndAddDBColumns
		stock_test.commitPriceHistory(pr_hist_report) # commit the price history report to a db to test
		cols_to_add = ['Winner','Loser']
		added_cols = stock_test.checkAndAddDBColumns(cols_to_add, "TenYrPrices")
		
		assert added_cols[0] is True, "Failed to add columns to DB table. Returned %r" % added_cols[1]
		assert isinstance(added_cols, tuple), "Added cols came back as %r instead of tuple." % type(added_cols)
		
		# query db for added cols
		db_cols = [member[0] for member in stock_test.dbcnx[1].execute("SELECT * FROM TenYrPrices;").description] # need to update dbcnx from time to time with a commit
     	# test selection of all tables from DB
		required_cols = list(pr_hist_report.columns) + cols_to_add	
		assert all(i in db_cols for i in required_cols), "Some columns were not added using checkAndAddDBColumns: got %r" % db_cols


		# test the retrieval of all db tables
		tables_to_add = ['BigYacht','DoGood']
		expected_tables = tables_to_add + ['TenYrPrices'] # cannot use append
		# add in a few more tables
		for i in tables_to_add:
			create_table_sql = "CREATE TABLE IF NOT EXISTS {table} (id integer PRIMARY KEY);".format(table=i)
			stock_test.dbcnx[1].execute(create_table_sql)
		# query the db for the tables
		all_tables = stock_test.testDBTables(stock_test.dbcnx[1])
		assert isinstance(all_tables, tuple), "testDBTables returned %r instead of a tuple." % type(all_tables)
		assert all_tables[0] == len(expected_tables), "Not all tables were returned from testDBTables. Got %r back." % all_tables[0]
		assert all(i[0] in expected_tables for i in all_tables[1]), "Some tables were not added to the DB. Got back %r." % all_tables[1]


		# test drop all tables
		dropped = stock_test.dropAllTables()
		assert dropped is True, "Table drop failed. Returned %r." % dropped[1]
		# query DB for tables and test len.
		all_db_tables = stock_test.dbcnx[1].execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;').fetchall() # 'SELECT * FROM all_stocks_key')
		assert len(all_db_tables) is 0, "Failed to drop all tables. Got back %r tables." % len(all_db_tables)

		# test tear down of db connection 
		print('\n' + str(stock_test.closeDBConnection(stock_test.dbcnx[0])))
			# todo: test if dbconnection closed
		# delete db		
		os.remove(test_db_2)

		return 'Successfully completed tests.'
	
	except Exception as e: 
		return ('\n!!!! A problem occured !!!\n', e, '\n')


if __name__ == "__main__":
	message = stockDBClassTests()

	if isinstance(message, tuple):
		for i in message:
			print (i)
	else: print('\n' + message + '\n')
