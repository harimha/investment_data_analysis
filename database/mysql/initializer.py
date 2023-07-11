import pandas as pd
from database.mysql.indicies.tables import IndexInfo, IndexCode, IndexOHLCV, IndexPER_PBR_DIV, IndexComponents
from database.mysql.stock.tables import StockCode, StockOHLCV_NAVER, StockDetails, StockBasicInfo, StockForeignHoldings, StockPER_PBR_DIV
from utils.datetimes import get_last_buisiness_day
from utils.utils import measure_execution_time
from database.mysql.stock.utils import scode_to_name
from database.mysql.indicies.utils import get_base_date


def initialize_code_db():
    icode = IndexCode()
    scode = StockCode()
    for obj in [icode, scode]:
        exe_time = measure_execution_time(obj.store_data)[1]
        print(f"{obj.table_name} is initialized ({exe_time}sec)")

def initialize_info_db():
    iinfo = IndexInfo()
    sinfo = StockBasicInfo()
    for obj in [iinfo, sinfo]:
        exe_time = measure_execution_time(obj.store_data)[1]
        print(f"{obj.table_name} is initialized ({exe_time}sec)")

def initialize_stock_db():
    ohlcv = StockOHLCV_NAVER()
    sinfo = StockBasicInfo()
    info_df = sinfo.read_db(["단축코드", "상장일"])
    edate = get_last_buisiness_day()

    for i in range(len(info_df)):
        short_code, sdate = info_df.iloc[i]
        stock_name = scode_to_name(short_code)
        execution_time = measure_execution_time(ohlcv.store_data_period_naver, stock_name, sdate, edate)[1]

        print(f"{i + 1}/{len(info_df)} {stock_name} is stored ({execution_time}sec)")

def initialize_index_db():
    ohlcv = IndexOHLCV()
    edate = get_last_buisiness_day()
    i_lst = ["코스피", "코스피200", "코스닥", "코스닥150"]
    i = 1
    for index_name in i_lst:
        sdate = get_base_date(index_name)
        execution_time = measure_execution_time(ohlcv.store_data_period,index_name,sdate,edate)[1]
        print(f"{i}/{len(i_lst)} {index_name} is stored ({execution_time}sec)")
        i+=1

