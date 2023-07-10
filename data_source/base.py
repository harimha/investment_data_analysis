from abc import abstractmethod, ABC
from utils.exceptions import WebResponseError, EmptyDataFrame


class Web(ABC):
    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    def _check_response(self, resp):
        if resp.status_code == 200:
            pass
        else:
            raise WebResponseError(resp.status_code, resp.text)

    def _set_params(self):
        params = {}
        params.update(self._params)

        return params

    def _check_empty_dataframe(self, df):
        if len(df) == 0:
            raise EmptyDataFrame
        else: pass


class NAVER(Web):
    def __init__(self):
        self._url = "https://api.finance.naver.com/siseJson.naver"
        self._params= {}

    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass


class KRX(Web):
    def __init__(self):
        self._params = {"locale": "ko_KR"}
        self._url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass








