from data_source.ecos.rawdata.core import 통계표조회
from preprocessing import dfhandling


class 주요국통화의대원화환율(통계표조회):
    def __init__(self):
        super().__init__()
        self.stat_code = "731Y001"
        self.period_type = "일"

    def won_dollar(self, sdate, edate):
        df = super().get_data(self.stat_code, self.period_type, sdate, edate, "0000001")
        df = df[["시점", "값"]]
        df.columns = ["일자", "원/달러"]
        col_datetime = ["일자"]
        col_float = ["원/달러"]
        df = dfhandling.change_type(df,
                                    datetime=col_datetime,
                                    float=col_float)

        return df

    def won_yen(self, sdate, edate):
        df = super().get_data(self.stat_code, self.period_type, sdate, edate, "0000002")
        df = df[["시점", "값"]]
        df.columns = ["일자", "원/100엔"]
        col_datetime = ["일자"]
        col_float = ["원/100엔"]
        df = dfhandling.change_type(df,
                                    datetime=col_datetime,
                                    float=col_float)
        return df

class 주요국통화의대미달러환율():
    def __init__(self):
        self.stat_code = "731Y002"
        self.period_type = "일"

