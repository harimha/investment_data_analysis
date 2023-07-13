from analysis.api import stock, indicies, exchange_rate, monetary
import matplotlib.pyplot as plt


def ratio_stockcap_m2():
    df_m2 = monetary.m2("201001", "202307")
    df_m2 = df_m2[["시점","M2(평잔, 원계열)"]]
    df_m2.columns = ["일자", "M2"]
    df_kospi = indicies.kospi_c("20100101", "20230713")
    df_merge = df_kospi.merge(df_m2, how="left", on="일자")
    df_merge = df_merge.fillna(method="ffill")
    df_merge = df_merge.dropna()

    df1 = df_merge.copy()
    df1["시총/m2"] = df1["상장시가총액"]/df1["M2"]
    df1 = df1.set_index("일자")
    df1["시총/m2"].plot()

ratio_stockcap_m2()