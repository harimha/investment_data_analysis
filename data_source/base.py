from abc import abstractmethod, ABC
from pandas.api.types import is_list_like
from utils.exceptions import WebResponseError, EmptyDataFrame
from config.config import KosisConfig, EcosConfig

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


class KOSIS(Web, KosisConfig):
    def __init__(self):
        super().__init__()
        self._params = {"apiKey": self._kosis_apikey,
                        "format": "json",
                        "jsonVD": "Y",
                        "jsonMVD": "Y"}

    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass


class FISIS(Web):
    def __init__(self):
        super().__init__()
        self._params = {"auth": self._fisis_apikey,
                        "lang": "kr"}

    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass




class ECOS(Web, EcosConfig):
    def __init__(self):
        super().__init__()
        self._url = "https://ecos.bok.or.kr/api"
        self._format = "json" # 요청 유형
        self._lang = "kr" # 언어구분
        self._spage = "1" # 요청시작건수
        self._epage = "100000" #요청종료건수

    def _set_params(self, params):
        parameters = self._params.copy()
        if is_list_like(params):
            for param in params:
                parameters.append(param)
        else:
            parameters.append(params)

        return parameters


    @abstractmethod
    def get_response(self):
        pass

    @abstractmethod
    def get_raw_data(self):
        pass

    @abstractmethod
    def get_data(self):
        pass







