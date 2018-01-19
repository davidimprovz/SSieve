# strings for accessing data sources 

class accessStrings():
	"""
    Properties for accessing stock data URLs
    """

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
