from analysis.api import *
import pandas as pd
import matplotlib.pyplot as plt


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


