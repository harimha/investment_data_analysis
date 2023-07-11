from database.mysql.stock.tables import *
from database.mysql.indicies.tables import *
from data_source.ecos.api.exchange_rate import 주요국통화의대원화환율

def won_dollar(sdate, edate):
    obj = 주요국통화의대원화환율()
    df = obj.won_dollar(sdate, edate)

    return df

def stock_ohlcv(stock_name, sdate=None, edate=None):
    obj = StockOHLCV_NAVER()
    df = obj.read_db(stock_name, obj.columns, sdate, edate)

    return df

def stock_c(stock_name, sdate=None, edate=None):
    obj = StockOHLCV_NAVER()
    df = obj.read_db(stock_name, ["일자", "종가"], sdate, edate)
    df.columns = ["일자", stock_name]

    return df

def kospi_c(sdate=None, edate=None):
    obj = IndexOHLCV()
    df = obj.read_db("코스피", ["일자", "종가", "상장시가총액"], sdate, edate)

    return df