from data_source.base import FISIS

class 통계정보(FISIS):
    def __init__(self):
        super().__init__()
        self.url = "http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json"

    def get_response(self):
        pass

    def get_raw_data(self):
        pass


    def get_data(self):
        pass
