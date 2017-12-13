# strings for accessing data sources 

class accessStrings():
	"""
    Properties for accessing stock data URLs
    """

	# NASDAQ URL formula for collecting all current exchange data in CSV format
	all_cur_stocks_csv_base_url = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange='
	all_cur_stocks_csv_exchange = ['nasdaq', 'nyse']
	all_cur_stocks_csv_tail = '&render=download'

	# AOL URL formula for recreating 10k/10q financials for 10yr/10qtr on any stock in CSV format. Requires a cookie to work.
	aol_fin_csv_base_url = 'http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t='
	aol_fin_csv_exchange = ['XNAS:','XNYS:']
	aol_fin_csv_report_region = '&region=usa&culture=en-US&productcode=QS&cur=&client=aol&reportType='
	aol_fin_csv_report_type = ['is','bs','cf']
	aol_fin_csv_report_period = '&period=' 
	aol_fin_csv_report_freq_str = ['3','12']
	aol_fin_csv_tail = '&dataType=A&order=asc&columnYear=10&curYearPart=1st5year&rounding=3&view=raw&r=567970&denominatorView=raw'

	# Morningstar formula for recreating 10k/10q financials for 5yr/5qtr on any stock in CSV format. backup in AOL doesn't work
	mngstar_fin_csv_base_url = 'http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t='
	mngstar_fin_csv_exchange = ['XNAS:','XNYS:']
	mngstar_fin_csv_report_region = '&region=usa&culture=en-US&cur=&reportType='
	mngstar_fin_csv_report_type = ['is','bs','cf']
	mngstar_fin_csv_report_period = '&period=' 
	mngstar_fin_csv_report_freq_str = ['3','12']
	mngstar_fin_csv_tail = '&dataType=A&order=asc&columnYear=5&curYearPart=1st5year&rounding=3&view=raw&denominatorView=raw&number=3'

	# Base URL for Morningstar 10yr pricing CSV 
	stock_price_mngstar_csv_base_url = 'http://performance.morningstar.com/perform/Performance/stock/exportStockPrice.action?t='
	stock_price_mngstar_csv_exchange = ['XNAS:', 'XNYS:']
	stock_price_mngstar_csv_period = ['&pd=10y', '&pd=ytd'] # this can be adjusted to 5D, YTD, 5y, etc in the future
	stock_price_mngstar_csv_freq_str= '&freq='
	stock_price_mngstar_csv_freq_period = ['d','w','m','a'] # can adjust freq=period
	stock_price_mngstar_csv_tail = '&sd=&ed=&pg=0&culture=en-US&cur=USD'

	# URL formula for Morningstar individual stock dividend tables for the past n years. 
	stock_div_table_mngstar_head = 'http://performance.morningstar.com/perform/Performance/stock/'
	stock_div_table_mngstar_type = ['upcoming-dividends','dividend-history']
	stock_div_table_mngstar_action = '.action?&t='
	stock_div_table_mngstar_exchange = ['XNAS:','XNYS:']
	stock_div_table_mngstar_region = '&region=usa&culture=en-US&cur=&ops=clear&ndec=2'
	stock_div_table_mngstar_tail = '&y=' # don't forget to specify the number of years of histrical data as 5, 10, 20, etc

	# path to retrieve the Morningstar CSV with all key financial data for 10-yr period
	stock_financials_mngstar_head = 'http://financials.morningstar.com/finan/ajax/exportKR2CSV.html?&callback=?&t='
	stock_financials_mngstar_exchange = ['XNAS:','XNYS:']
	stock_financials_mngstar_tail = '&region=usa&culture=en-US&cur=&order=asc'
