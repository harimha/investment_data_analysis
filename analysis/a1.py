from analysis.api import *
import pandas as pd
import matplotlib.pyplot as plt
# so = stock_c("삼성전자", "20210101", "20230101")


def 달러대비시총누적수익률비교(sdate, edate):
    kospi = kospi_c(sdate, edate)[["일자", "상장시가총액"]]
    wd = won_dollar(sdate, edate)
    df_merge = kospi.merge(wd, how="left", on="일자")
    df_merge = df_merge.set_index("일자")
    df_merge["상장시총_달러"] = df_merge["상장시가총액"] / df_merge["원/달러"]
    df_merge["시총누적수익률_원"] = ((df_merge["상장시가총액"] / df_merge["상장시가총액"].iloc[0]) - 1) * 100
    df_merge["시총누적수익률_달러"] = ((df_merge["상장시총_달러"] / df_merge["상장시총_달러"].iloc[0]) - 1) * 100
    df_merge["시총_달러_원_diff"] = df_merge["시총누적수익률_달러"] - df_merge["시총누적수익률_원"]
    # df_merge["시총_달러_원_diff"].plot()
    # plt.show()

    return df_merge

df = 달러대비시총누적수익률비교("20050101", "20230712")
df.head()
df["시총_달러_원_diff_30"] = df["시총_달러_원_diff"].shift(30)
df["기울기"] = df["시총_달러_원_diff"] - df["시총_달러_원_diff_30"]
df["기울기"].plot()
df["상장시가총액"][df["기울기"]<-20]

df["코스피저점"] = df["상장시가총액"][df["기울기"]<-15]
df["상장시가총액"].plot()
df["코스피저점"].plot()

df["mean"]=df["시총_달러_원_diff"].rolling(60).mean()
df["std"]=df["시총_달러_원_diff"].rolling(60).std()
df["upper"]=df["mean"]+3*df["std"]
df["lower"]=df["mean"]-3*df["std"]
df["mean"].plot()
df["upper"].plot()
df["lower"].plot()
df["시총_달러_원_diff"].plot()

