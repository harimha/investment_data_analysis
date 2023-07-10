import pandas as pd

from db.indicies.tables import IndexInfo, IndexCode, IndexPER_PBR_DIV
from db.stock.tables import StockCode, StockOHLCV, StockBasicInfo

from krxdata.db.stock.utils import scode_to_name
from krxdata.db.indicies.utils import get_base_date
from krxdata.db.utils import get_last_buisiness_day
import time

def initialize_code_db():
    icode = IndexCode()
    scode = StockCode()
    for obj in [icode, scode]:
        print(f"{obj.table_name} is being initialized")
        obj.store_data()

def initialize_info_db():
    iinfo = IndexInfo()
    sinfo = StockBasicInfo()
    for obj in [iinfo, sinfo]:
        print(f"{obj.table_name} is being initialized")
        obj.store_data()


def initialize_index_db(index_lst):
    obj = IndexPER_PBR_DIV()
    edate = get_last_buisiness_day()
    i = 1
    for index in index_lst:
        print(f"{i}/{len(index_lst)}")
        sdate = get_base_date(index)
        obj.store_data_period(index, sdate, edate, 0.1)
        i += 1

def initialize_stock_db():
    ohlcv = StockOHLCV(naver=True)
    sinfo = StockBasicInfo()
    df = sinfo.read_db(["단축코드", "상장일"])
    # df = sinfo.read_db(["단축코드", "상장일", "시장구분"])
    # df = df[["단축코드", "상장일"]][df["시장구분"] == market]
    edate = get_last_buisiness_day()
    avg_time = []
    for i in range(len(df)):

        start_time = time.time()

        short_code, sdate = df.iloc[i]
        stock_name = scode_to_name(short_code)

        df_data = ohlcv.get_data(stock_name, sdate, edate)
        df_db = ohlcv.read_db(stock_name, ohlcv.columns)

        df_unique = df_data.merge(df_db, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)
        df_unique.to_sql(ohlcv.table_name, ohlcv.engine, ohlcv.schema_name,index=False,if_exists="append")

        end_time = time.time()
        execution_time = end_time - start_time
        avg_time.append(execution_time)
        print(f"{i + 1}/{len(df)} {stock_name} is stored {round(execution_time,2)}초")

    return avg_time

def initialize_stock_db():
    ohlcv = StockOHLCV(naver=True)
    sinfo = StockBasicInfo()
    df = sinfo.read_db(["단축코드", "상장일"])
    # df = sinfo.read_db(["단축코드", "상장일", "시장구분"])
    # df = df[["단축코드", "상장일"]][df["시장구분"] == market]
    edate = get_last_buisiness_day()

    for i in range(len(df)):
        short_code, sdate = df.iloc[i]
        stock_name = scode_to_name(short_code)

        df_data = ohlcv.get_data(stock_name, sdate, edate)
        df_db = ohlcv.read_db(stock_name, ohlcv.columns)

        df_unique = df_data.merge(df_db, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)
        df_unique.to_sql(ohlcv.table_name, ohlcv.engine, ohlcv.schema_name,index=False,if_exists="append")
        print(f"{i + 1}/{len(df)} {stock_name} is stored")


