import time
import datetime as dt
import sys
import os
import pandas as pd
import ccxt
from testConfig import *


# 获取交易所原始数据
def getRecords(ex, symbol, level, startTime, endTime):
    dfList = []  # 存储所有原始数据的list
    startTime += " 00:00:00"
    endTime += " 00:00:00"
    sinceTime= startTime

    while True:
        print(f"Getting {symbol} {level} {sinceTime}...")
        # 从开始时间取数据，单次循环能取到交易所允许的最大数据量
        data = ex.fetch_ohlcv(symbol=symbol, timeframe=level, since=ex.parse8601(sinceTime), limit=1500)
        # 如果本次循环没取到则退出循环
        if not data: break
        # 每次循环取到的list都叠加到dfList中暂放
        dfList += data
        # 取最后一根K线的时间作为下次开始时间，如果该时间到达截至时间或最新时间则退出循环
        t = pd.to_datetime(data[-1][0], unit="ms")
        endTime = pd.to_datetime(endTime)
        if t >= endTime or len(data) <= 1: break
        sinceTime = str(t)
        time.sleep(SLEEP_SHORT)
    
    # 把临时list转换成pandas数据并对列命名，直接返回空数据组
    dfList =  pd.DataFrame(dfList, dtype=float)
    if dfList.empty: return dfList
    
    dfList.rename(columns={
        0: "openTime",
        1: "open",
        2: "high",
        3: "low",
        4: "close",
        5: "volume",
    }, inplace=True)
    # 增加一列北京时间
    dfList["openTimeGmt8"] = pd.to_datetime(dfList["openTime"], unit="ms") + dt.timedelta(hours=8)
    dfList = dfList[["openTime", "open", "high", "low", "close", "volume", "openTimeGmt8"]]
    # 用时间进行去重、排序、去掉合并数据的行标号、去掉时间范围外的数据
    dfList.drop_duplicates(subset=["openTimeGmt8"], keep="last", inplace=True)
    dfList.sort_values("openTime", inplace=True)
    _start = pd.to_datetime(startTime) + dt.timedelta(hours=8)
    _end = pd.to_datetime(endTime) + dt.timedelta(hours=8)
    dfList = dfList[(dfList["openTimeGmt8"] >= _start) & (dfList["openTimeGmt8"] <= _end)]
    dfList.reset_index(drop=True, inplace=True)
    # 去掉最新K线，只取收盘价确定的K线
    # dfList = dfList[:-1]
    return dfList


def main():
    # python getKlines.py binance spot btc/usdt 5m 2019-11-12 2022-12-13
    EXCHANGE_CONFIG = {"timeout": 5000,}
    
    if len(sys.argv) < 7:
        print("Usage:")
        print("    python getKlines.py binance spot btc/usdt 5m 2019-11-12 2022-12-13")
        print("    python getKlines.py binance swap btc/usdt 5m 2019-11-12 2022-12-13")
        raise RuntimeError("参数不正确")
    if sys.argv[2] == "swap":
        EXCHANGE_CONFIG["options"]={"defaultType":"future"}

    ex = ccxt.binance(EXCHANGE_CONFIG)
    _type = sys.argv[2]
    symbol = sys.argv[3].upper()
    level = sys.argv[4].lower()
    dateStart = sys.argv[5]
    dateEnd = sys.argv[6]

    dataPath = "dataStore"

    df = getRecords(ex, symbol, level, dateStart, dateEnd)
    dateStartReal = df.loc[0, "openTimeGmt8"].date()
    dateEndReal = df.loc[df.shape[0]-1, "openTimeGmt8"].date()

    file = os.path.join(dataPath, f"data_{symbol.replace('/', '-')}_{_type}_{level}_{dateStartReal}_{dateEndReal}.hdf")
    print(f"完成获取k线，共{df.shape[0]}根")
    df.to_hdf(file, key="df", index=False, mode="w")
    print(f"完成写入文件。起始日期：{dateStartReal},结束日期：{dateEndReal}")


if __name__ == "__main__":
    main()
