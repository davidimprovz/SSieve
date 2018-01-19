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

