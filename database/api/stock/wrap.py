from database.mysql.stock.tables import *

def stock_c(stock_name, sdate=None, edate=None):
    obj = StockOHLCV_NAVER()
    df= obj.read_db(stock_name, ["일자", "종가"], sdate, edate)

    return df

