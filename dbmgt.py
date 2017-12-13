# db management

import sqlite3, os, sys

class stockDB():
	"""
	Handles db setup and admin for stock scraping daemon.
	This class is intended to be used by either the initializing
	routine or by the daily / monthly update routines as an 
	inherited class. 

	"""
	
	def __init__(self, connection='~/AUTOSIFT/ez_equity_daemon/scraping/second_xchanges_trial.db'):
		"""
		Takes a single string argument of a new or 
		existing db location on disk.

		The initial state is a string of the location you would like 
		to use for your DB. You must call the connectToDB() method
		after class instantiation to actually connect to this DB.
		"""
		self._dbcnx = connection

	# store the DB connection reference
	@property
	def dbcnx(self):
		"""
		Return read-only copy of the dbcnx.
		"""
		return self._dbcnx

	@dbcnx.setter
	def dbcnx(self, connection):
		"""
		Set the dbcnx property of the class instance. 
		This should be called only once immediately after the 
		class is instantiated. 
		"""
		self._dbcnx = connection
	
	# connectToDB
    # *********** #
	def connectToDB(self, connection='~/AUTOSIFT/ez_equity_daemon/scraping/second_xchanges_trial.db'): 
		"""
		Connect to the desired DB. Note that this file must exist. If no DB specified,
		connect to the primary stock database. 

		Set a database path for the stock data you're about to collect. 

		This function takes one string, an absolute path to your desired database location
		with the format /[folder]/[sub-folder]/[yourfile].db
		Note that there is no path checking built into this function. Make sure your path 
		works before calling the function. 

		Use the connection variable to pass in a string of the sqlite file name you desire.

		Returns a tuple reference to the connection and cursor objects.

		Example usage: 
		"""
		
		try:
			# make sure it's a string path to db file
			assert isinstance(connection, str), "expected path to file, got %r instead." % type(connection)
			# hard coded value to be passed in through user login later
			cnx = sqlite3.connect(connection)
			# get the cursor element
			cur = cnx.cursor()
			# find out if the db is new or existing
			message = self.testDBTables(cur)

			if message[0] is 0: # a new db
				message = 'new db started: {conn}'.format(conn=connection)
			else: # an existing db
				message = 'existing db ready: {conn}.'.format(conn=connection)
			
			# set the db connection
			self.dbcnx = (cnx, cur, message)
			
			return message
		
		except Exception as e:
			return (False, e)

	# closeDBConnection()
    # ******************* #
	def closeDBConnection(self, connection):
		"""
		Pass in reference to self.dbcnx[0].

		Remember that when you're done creating your database, you will want to call the closing methods 
		to commit all changes and close the connection. Failing to do so can sometimes have negative 
		consequences, as in the complete FUBARing of your database. 

		Accepts the connection tuple returned by connectToDB. 

		Example usage: 
		"""
		try:
			connection.commit()
			connection.close()
			return 'DB connection successfully closed.'

		except Exception as e:
			return (False, e)

	# dropAllTables()
    # *************** #
	def dropAllTables(self):
		"""
		Danger! This function drops all tables in your currently active DB.

		Pass in refernce to self.dbcnx[1].

		Use this function to erase the information in your current DB and start over. 
		Probably a good idea to remove this function for your production code.

		Returns True if successfully remove all tables. Otherwise, returns a tuple
		of (False, error message).
		
		Example Usage: 
		"""

		try:
			all_db_tbls = self.dbcnx[1].execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;').fetchall()

			for i in all_db_tbls:
				self.dbcnx[1].execute('DROP TABLE IF EXISTS {tbl}'.format(tbl = i[0]))
			return True

		except Exception as e:
			return False, e

	# checkAndAddDBColumns()
    # ********************** #
	def checkAndAddDBColumns(self, df_columns, table):
	    """
	    Trying to write an earlier date col to an existing table will result in an err.

	    This function is used in the data gather chain and called from commit* functions.
	    It checks columns in the table being written to. If the report you're trying to generate
	    has new columns that have not been inserted into the table yet, this function will 
	    insert those column names. Where there are gaps, a column is added. 

	    Accepts the dataframe column names of the dataframe you're trying to save to the db, 
	    as well as the table you're trying to save to. 

	    Returns True, added_columns if successful. Otherwise, returns False and error.
	    """
	    try:
	        # get a list of all columns in the db
	        existing = self.dbcnx[1].execute("SELECT * FROM {tbl};".format(tbl=table))
	        existing = [member[0] for member in existing.description]
	        
	        # compare names of columns to df_columns 
	        added_cols = []
	        for header in df_columns:
	            if header not in existing:
	                self.dbcnx[1].execute("ALTER TABLE '{tbl_name}' ADD COLUMN '{col_name}';".format(tbl_name = table, col_name = header))
	                added_cols.append(header)

	        return True, added_cols

	    except Exception as e:
	        return False, e

	# testDbTables()
    # ************** #
	def testDBTables(self, cursor):
		"""
		Check the DB to see which tables are there. 

		Accepts a cursor argument which you can get from connectToDB(). Should
		be self.dbcnx[1].

		Returns a tuple of table count and name of all tables.

		Example usage: 
		"""
	    # test to make sure all tables were created
		all_db_tables = cursor.execute('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;').fetchall()
		return (len(all_db_tables), all_db_tables)
