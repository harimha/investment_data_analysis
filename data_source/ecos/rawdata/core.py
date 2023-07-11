import requests as req
import pandas as pd
from data_source.base import ECOS


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


class 통계목록(ECOS):
    def __init__(self):
        super().__init__()
        self._service_name = "StatisticTableList"
        self._params = [self._url,
                        self._service_name,
                        self._apikey,
                        self._format,
                        self._lang,
                        self._spage,
                        self._epage]

    def get_response(self, stat_code):
        params = self._set_params(stat_code)
        url = "/".join(params)
        resp = req.get(url)

        return resp

    def get_raw_data(self, stat_code):
        resp = self.get_response(stat_code)
        resp = resp.json()[self._service_name]["row"]
        df = pd.DataFrame(resp)

        return df

    def get_data(self, stat_code):
        df = self.get_raw_data(stat_code)
        df.columns = ["상위통계표코드", "통계표코드", "통계명", "주기", "검색가능여부", "출처"]

        return df


class 통계세부항목(ECOS):
    def __init__(self):
        super().__init__()
        self._service_name = "StatisticItemList"
        self._params = [self._url,
                        self._service_name,
                        self._apikey,
                        self._format,
                        self._lang,
                        self._spage,
                        self._epage]

    def get_response(self, stat_code):
        params = self._set_params(stat_code)
        url = "/".join(params)
        resp = req.get(url)

        return resp

    def get_raw_data(self, stat_code):
        resp = self.get_response(stat_code)
        resp = resp.json()[self._service_name]["row"]
        df = pd.DataFrame(resp)

        return df

    def get_data(self, stat_code):
        df = self.get_raw_data(stat_code)
        df.columns = ["통계표코드", "통계명", "항목그룹코드", "항목그룹명", "통계항목코드",
                      "통계항목명", "상위통계항목코드", "상위통계항목명", "주기", "수록시작일자",
                      "수록종료일자", "자료수", "단위", "가중치"]

        return df


class 통계표조회(ECOS):
    def __init__(self):
        super().__init__()
        self._service_name = "StatisticSearch"
        self._params = [self._url,
                        self._service_name,
                        self._apikey,
                        self._format,
                        self._lang,
                        self._spage,
                        self._epage]

    def _get_period_type(self, type):
        if type == "년":
            return "A"
        elif type == "반년":
            return "S"
        elif type == "분기":
            return "Q"
        elif type == "월":
            return "M"
        elif type == "반월":
            return "SM"
        elif type == "일":
            return "D"
        else:
            return None

    def get_response(self, stat_code, period_type, sdate, edate, *itemcode):
        period_type = self._get_period_type(period_type)
        params = self._set_params([stat_code, period_type, sdate, edate, *itemcode])
        url = "/".join(params)
        resp = req.get(url)

        return resp


    def get_raw_data(self, stat_code, period_type, sdate, edate, *itemcode):
        resp = self.get_response(stat_code, period_type, sdate, edate, *itemcode)
        resp = resp.json()[self._service_name]["row"]
        df = pd.DataFrame(resp)

        return df

    def get_data(self, stat_code, period_type, sdate, edate, *itemcode):
        df = self.get_raw_data(stat_code, period_type, sdate, edate, *itemcode)
        df.columns = ["통계표코드", "통계명", "통계항목코드1", "통계항목명1", "통계항목코드2",
                      "통계항목명2", "통계항목코드3", "통계항목명3", "통계항목코드4", "통계항목명4",
                      "단위", "시점", "값"]

        return df


class 통계메타(ECOS):
    def __init__(self):
        super().__init__()
        self._service_name = "StatisticMeta"
        self._params = [self._url,
                        self._service_name,
                        self._apikey,
                        self._format,
                        self._lang,
                        self._spage,
                        self._epage]

    def get_response(self, data_name):
        params = self._set_params(data_name)
        url = "/".join(params)
        resp = req.get(url)

        return resp

    def get_raw_data(self, data_name):
        resp = self.get_response(data_name)
        resp = resp.json()[self._service_name]["row"]
        df = pd.DataFrame(resp)

        return df

    def get_data(self, data_name):
        df = self.get_raw_data(data_name)
        df.columns = ["레벨", "상위통계항목코드", "통계항목코드", "통계항목명", "메타데이터"]

        return df





