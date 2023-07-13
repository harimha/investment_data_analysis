from data_source.ecos.rawdata.subclass.exchange_rate import 주요국통화의대원화환율

def won_dollar(sdate, edate):
    obj = 주요국통화의대원화환율()
    df = obj.won_dollar(sdate, edate)

    return df