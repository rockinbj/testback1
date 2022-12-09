import datetime as dt
import math
import time
from itertools import product

import numpy as np
import pandas as pd
import requests

from testConfig import *

pd.set_option("display.expand_frame_repr", False)
pd.set_option("display.max_column", None)
pd.set_option("display.max_rows", 5000)
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)


# 获取交易所原始数据
def getRecords(ex, symbol, level, startTime, endTime):
    dfList = []  # 存储所有原始数据的list
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


# 将底层k线合成大周期k线
def rebuildCandles(df, level):
    level = level.upper()
    if "M" in level:
        level = level.replace("M", "T")
    
    dfNew = df.resample(rule=level, on="openTimeGmt8", label="left", closed="left").agg(
        {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    })
    dfNew.dropna(subset=["open"], inplace=True)
    dfNew = dfNew[dfNew["volume"]>0]
    dfNew.reset_index(inplace=True)
    dfNew = dfNew[["openTimeGmt8", "open", "high", "low", "close", "volume"]]
    dfNew.reset_index(inplace=True, drop=True)
    
    return dfNew


# 数据写入文件
def writeRecordsToFile(df, path, method="csv"):
    #建立存储目录
    # os.makedirs(path, exist_ok=True)
    # #按起始时间命名
    # start = df.iloc[0]["openTimeGmt8"]
    # end = df.iloc[-1]["openTimeGmt8"]
    if method == "csv":
        # fileName = os.path.join(path, f"{start}_{end}.csv".replace(" ","-").replace(":",""))
        # df.to_csv(fileName, index=False)
        df.to_csv(path, index=False)
    elif method == "hdf":
        # fileName = os.path.join(path, f"{start}_{end}.h5".replace(" ","-").replace(":",""))
        # df.to_hdf(fileName, key="df", mode="w", index=False)
        df.to_hdf(path, key="df", mode="w", index=False)


# 将给定时间段拆分成若干小时，用来并发
def splitTime(startTime, endTime, frequency):
    startTime, endTime = pd.to_datetime(startTime), pd.to_datetime(endTime)
    time_range = list(pd.date_range(startTime, endTime, freq='%sS' % frequency))
    if endTime not in time_range:
        time_range.append(endTime)
    time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
    time_ranges = []
    for item in time_range:
        f_time = item
        t_time = (dt.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + dt.timedelta(seconds=frequency))
        if t_time >= endTime:
            t_time = endTime.strftime("%Y-%m-%d %H:%M:%S")
            time_ranges.append([f_time, t_time])
            break
        time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
    return time_ranges


# 生成所有参数组合
def getParas(parasList):
    return list(product(*parasList))


# 简单布林策略信号计算
def getSignalBolling(df, para):
    # para:
    # [maLength, times]
    # [400, 2]
    
    maLength = para[0]
    times = para[1]

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    df["ma"] = df["close"].rolling(maLength).mean()
    df["stdDev"] = df["close"].rolling(maLength).std(ddof=0)
    df["upper"] = df["ma"] + times * df["stdDev"]
    df["lower"] = df["ma"] - times * df["stdDev"]

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = df["close"].shift(1) <= df["upper"].shift(1)
    condLong2 = df["close"] > df["upper"]
    df.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = df["close"].shift(1) >= df["ma"].shift(1)
    condCoverLong2 = df["close"] < df["ma"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = df["close"].shift(1) >= df["lower"].shift(1)
    condShort2 = df["close"] < df["lower"]
    df.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = df["close"].shift(1) <= df["ma"].shift(1)
    condCoverShort2 = df["close"] > df["ma"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)
    
    return df


# 平均差布林策略信号计算
def getSignalBollingMean(df, para):
    # para:
    # [maLength, times]
    # [400, 2]
    
    maLength = para[0]
    times = para[1]

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    df["ma"] = df["close"].rolling(maLength).mean()
    df["diff"] = abs(df["close"] - df["ma"])
    df["diffMean"] = df["diff"].rolling(maLength).mean()
    df["upper"] = df["ma"] + times * df["diffMean"]
    df["lower"] = df["ma"] - times * df["diffMean"]

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = df["close"].shift(1) <= df["upper"].shift(1)
    condLong2 = df["close"] > df["upper"]
    df.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = df["close"].shift(1) >= df["ma"].shift(1)
    condCoverLong2 = df["close"] < df["ma"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = df["close"].shift(1) >= df["lower"].shift(1)
    condShort2 = df["close"] < df["lower"]
    df.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = df["close"].shift(1) <= df["ma"].shift(1)
    condCoverShort2 = df["close"] > df["ma"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    # 填充signal的空白
    # df["signal"].fillna(method="ffill", inplace=True)
    # df["signal"].fillna(value=0, inplace=True)
    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)
    
    return df


# 布林增加延迟开仓，用的pipe，较慢
def getSignalBollingDelay2(df, para):
    # para:
    # [maLength, times, percent]
    # [400, 2, 3]
    # 产生开仓信号，并且，上轨或者下轨距离中轨的距离要小于percent，才开仓
    
    maLength = para[0]
    times = para[1]
    percent = para[2]

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    df["ma"] = df["close"].rolling(maLength).mean()
    df["stdDev"] = df["close"].rolling(maLength).std(ddof=0)
    df["upper"] = df["ma"] + (times * df["stdDev"])
    df["lower"] = df["ma"] - (times * df["stdDev"])
    df["diff"] = abs(df["close"] / df["ma"] - 1)
    df["isInDiff"] = df["diff"].map(lambda x: 1 if x<(percent/100) else 0)

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = df["close"].shift(1) <= df["upper"].shift(1)
    condLong2 = df["close"] > df["upper"]
    df.loc[condLong1 & condLong2, "willLong"] = 1
    
    condCoverLong1 = df["close"].shift(1) >= df["ma"].shift(1)
    condCoverLong2 = df["close"] < df["ma"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = df["close"].shift(1) >= df["lower"].shift(1)
    condShort2 = df["close"] < df["lower"]
    df.loc[condShort1 & condShort2, "willShort"] = -1

    condCoverShort1 = df["close"].shift(1) <= df["ma"].shift(1)
    condCoverShort2 = df["close"] > df["ma"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0
    
    # 满足开仓条件时willLong置1，如果此时距离参数isInDiff为0，则复制willLong直到isInDiff为1
    def copyWillLongTillInDiff(_df1):
        for i,r in _df1.iterrows():
            preDiff = _df1.loc[max(i-1,0), "isInDiff"]
            preWillLong = _df1.loc[max(i-1,0), "willLong"]
            preWillShort = _df1.loc[max(i-1,0), "willShort"]
            if (preDiff==0 and preWillLong==1):
                _df1.loc[i, "willLong"] = _df1.loc[i-1, "willLong"]
            if (preDiff==0 and preWillShort==-1):
                _df1.loc[i, "willShort"] = _df1.loc[i-1, "willShort"]

    df.pipe(copyWillLongTillInDiff)

    # 当willLong(Short)和isInDiff都为1时，触发signalLong(Short)
    df.loc[(df["willLong"]==1)&(df["isInDiff"]==1), "signalLong"] = 1
    df.loc[(df["willShort"]==-1)&(df["isInDiff"]==1), "signalShort"] = -1
    
    # 填充signal的空白
    # df["signal"].fillna(method="ffill", inplace=True)
    # df["signal"].fillna(value=0, inplace=True)
    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)

    return df


# 布林增加延迟开仓
def getSignalBollingDelay(df, para):
    # para:
    # [maLength, times, percent]
    # [400, 2, 3]
    # 产生开仓信号，并且，上轨或者下轨距离中轨的距离要小于percent，才开仓
    
    maLength = para[0]
    times = para[1]
    percent = para[2] / 100

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    df["ma"] = df["close"].rolling(maLength).mean()
    df["stdDev"] = df["close"].rolling(maLength).std(ddof=0)
    df["upper"] = df["ma"] + times * df["stdDev"]
    df["lower"] = df["ma"] - times * df["stdDev"]
    df["dif"] = abs(df["close"] / df["ma"] - 1)

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = df["close"].shift(1) <= df["upper"].shift(1)
    condLong2 = df["close"] > df["upper"]
    df.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = df["close"].shift(1) >= df["ma"].shift(1)
    condCoverLong2 = df["close"] < df["ma"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = df["close"].shift(1) >= df["lower"].shift(1)
    condShort2 = df["close"] < df["lower"]
    df.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = df["close"].shift(1) <= df["ma"].shift(1)
    condCoverShort2 = df["close"] > df["ma"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    # 填充signal的空白
    # df["signal"].fillna(method="ffill", inplace=True)
    # df["signal"].fillna(value=0, inplace=True)
    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]

    # 修改开仓信号，增加延迟开仓的约束
    df["signal2"] = df["signal"]
    # 复制一个新的信号列，并复制填充，形成一个信号向下连续的信号列
    df["signal2"].fillna(method="ffill", inplace=True)
    # 把原信号列中1、-1的信号清空，等待用复制列的信号反填充回来
    df.loc[df["signal"]!=0, "signal"] = None
    # 只有在满足percent约束且是1、-1信号时，才将复制信号列反填充回来
    # 由于新的信号列经过了ffill的填充，也就实现了向下复制信号的效果
    cond1 = df["signal2"] == 1
    cond2 = df["signal2"] == -1
    df.loc[(cond1 | cond2)&(df["dif"]<=percent), "signal"] = df["signal2"]

    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)
    
    return df


def getSignalSma3(df, para):
    ma1Len = para[0]
    ma2Len = para[1]
    ma3Len = para[2]
    dist = para[3] / 100

    # 计算三均线，MA均线
    df["ma1"] = df["close"].rolling(ma1Len).mean()
    df["ma2"] = df["close"].rolling(ma2Len).mean()
    df["ma3"] = df["close"].rolling(ma3Len).mean()

    # EMA均线
    # df["ma1"] = pd.ewma(df["close"], span=ma1Len)
    # df["ma2"] = pd.ewma(df["close"], span=ma2Len)
    # df["ma3"] = pd.ewma(df["close"], span=ma3Len)

    # 做多平多信号，多头排列做多，ma1下穿ma2平多
    condLong1 = df["ma1"] > df["ma2"]
    condLong2 = df["ma2"] > df["ma3"]
    condLong3 = (df["ma1"] / df["ma3"] - 1) > dist
    df.loc[(condLong1&condLong2)&condLong3, "signalLong"] = 1

    condCoverLong1 = df["ma1"].shift() > df["ma2"].shift()
    condCoverLong2 = df["ma1"] < df["ma2"]
    df.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    condShort1 = df["ma1"] < df["ma2"]
    condShort2 = df["ma2"] < df["ma3"]
    condShort3 = (df["ma3"] / df["ma1"] -1) > dist
    df.loc[(condShort1&condShort2)&condShort3, "signalShort"]  = -1

    condCoverShort1 = df["ma1"].shift() < df["ma2"].shift()
    condCoverShort2 = df["ma1"] > df["ma2"]
    df.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)

    return df


def getSignalNwe(df, para):
    nweLength = para[0]
    nweBandwidth = para[1]
    nweTimes = para[2]
    atrLength = para[3]
    atrTimes = para[4]
    rsiLength = para[5]
    plRate = para[6]

    def getNwe(dfClose, bandwidth, times):

        close = list(dfClose)
        close.reverse()
        length = len(close)

        y = []
        sum_e = 0.0

        for i in range(length):
            sum = 0.0
            sumw = 0.0

            for j in range(length):
                w = math.exp(-(math.pow(i-j,2)/(bandwidth*bandwidth*2)))
                sum += close[j]*w
                sumw += w
            
            y2 = sum / sumw
            sum_e += abs(close[i] - y2)
            y.append(y2)
        
        mae = sum_e / length * times
        # 直接改写外部df，把本次结果写入最后一行，apply一组轮转500行，结果是最后一行的
        nwe = y[0]
        nweUpper = nwe + mae
        nweLower = nwe - mae
        
        # rolling apply只允许返回一个数字，用下面方法可以访问到外部df的index，就可以直接修改外部df
        _index = dfClose.index[-1]
        df.loc[_index, "nweMed"] = nwe
        df.loc[_index, "nweUpper"] = nweUpper
        df.loc[_index, "nweLower"] = nweLower
        return 1


    def getAtr(df, length=14, times=0.5):
        df["atr"] = df.ta.atr(length=length) * times
        df["atrHigh"] = df["high"] + df["atr"]
        df["atrLow"] = df["low"] - df["atr"]

        return df


    def getRsi(df, length=5):
        df["rsi"] = df.ta.rsi(length=length)
        return df


    # 因为rolling.apply只能返回一个数值，用下面方法直接把nwe轨道写入df。那么也不需要返回值了。虽然写法很怪异。。。
    # https://stackoverflow.com/questions/60736556/pandas-rolling-apply-using-multiple-columns/60918101#60918101
    rol = df["close"].rolling(nweLength)
    rol.apply(getNwe, raw=False, args=(nweBandwidth, nweTimes))  # 此时df已经带有nwe轨道值了

    df = getAtr(df, atrLength, atrTimes)

    df = getRsi(df, rsiLength)

    # 开多：收盘价上穿nwe下轨，且rsi超卖
    distToMed = 0.2  # 调整到中轨的距离
    condLong1 = df["close"].shift() > df["nweLower"].shift()
    condLong2 = df["close"] < df["nweLower"]
    condLong3 = df["rsi"] < 30
    condLong4 = df["open"] < df["nweMed"]-(df["nweMed"]-df["nweLower"])*distToMed
    df.loc[condLong1&condLong2&condLong3&condLong4, "signalLong"] = 1

    # 开空：收盘价下穿nwe上轨，且rsi超买
    condShort1 = df["close"].shift()<df["nweUpper"].shift()
    condShort2 = df["close"]>df["nweUpper"]
    condShort3 = df["rsi"] > 70
    condShort4 = df["open"] > df["nweUpper"]-(df["nweUpper"]-df["nweMed"])*(1-distToMed)
    df.loc[condShort1&condShort2&condShort3&condShort4, "signalShort"] = -1

    #计算延迟开仓和止盈止损的函数
    # 止损：atr下轨
    # 止盈：固定比例止盈plRate
    # 延迟开仓约束：
    # 1、出现开仓信号（到达轨道边缘） 
    # 2、出现反转k线回穿边缘（出反转趋势） 
    # 3、反转k线的收盘价没有越过中轨（规避趋势行情，震荡策略怕趋势）,距中轨20%距离
    def delayOpen(_df1, plRate, steps=5):
        indexs = _df1.index.tolist()
        for index in indexs:
            for i in range(1, steps+1):

                if (index+i)>(len(df)-1): break  # 防止末尾有计算超出范围

                if (df.loc[index, "signalLong"]==1)\
                    and (df.loc[index+i, "close"]>df.loc[index+i, "nweLower"])\
                    and df.loc[index+i, "close"]<df.loc[index+i,"nweMed"]-(df.loc[index+i,"nweMed"]-df.loc[index+i,"nweLower"])*distToMed:
                    df.loc[index+i, "signal"] = 1
                    df.loc[index+i, "stopLossLong"] = df.loc[index+i, "atrLow"]
                    df.loc[index+i, "stopProfitLong"] = df.loc[index+i, "close"]+(df.loc[index+i, "close"]-df.loc[index+i, "atrLow"])*plRate
                    break

                elif (df.loc[index, "signalShort"]==-1)\
                    and (df.loc[index+i, "close"]<df.loc[index+i, "nweUpper"])\
                    and df.loc[index+i,"close"]>df.loc[index+i, "nweUpper"]-(df.loc[index+i,"nweUpper"]-df.loc[index+i,"nweMed"])*(1-distToMed):
                    df.loc[index+i, "signal"] = -1
                    df.loc[index+i, "stopLossShort"] = df.loc[index+i, "atrHigh"]
                    df.loc[index+i, "stopProfitShort"] = df.loc[index+i, "close"]-(df.loc[index+i, "atrHigh"]-df.loc[index+i, "close"])*plRate
                    break

        # apply必须返回一个值
        return 1
    
    # 把带开仓信号的k线放入计算函数，向后推steps根，如果出现反转k线就开仓并计算当下的止盈止损，如果没出现就忽略
    steps = 5
    r = df.loc[(df["signalLong"]==1)|(df["signalShort"]==-1)].apply(delayOpen, args=(plRate, steps))
    
    def stopLossProfit(_df2):
        indexs = _df2.index.tolist()
        for index in indexs:
            n = 1
            while True:
                # 超限退出
                if (index+n) > (len(df)-1): break
                # 出现与当前持仓方向不同的信号就退出，相同持仓时不退出继续
                # 避免出现止盈止损点位被后覆盖
                if pd.notnull(df.loc[index+n, "signal"])\
                    and df.loc[index+n, "signal"] != df.loc[index, "signal"]: 
                    break

                if df.loc[index, "signal"]==1:
                    if df.loc[index+n, "low"] <= df.loc[index, "stopLossLong"]\
                        or df.loc[index+n, "high"] >= df.loc[index, "stopProfitLong"]:
                        df.loc[index+n, "signal"] = 0
                        break
                elif df.loc[index, "signal"]==-1:
                    if df.loc[index+n, "high"] >= df.loc[index, "stopLossShort"]\
                        or df.loc[index+n, "low"] <= df.loc[index, "stopProfitShort"]:
                        df.loc[index+n, "signal"] = 0
                        break
                n += 1
        return 1 
    
    # 有信号的k线即带止盈止损价位的k线，放入止盈止损函数进行处理
    # 止盈止损函数里会一直向后寻找止盈止损出场k线，
    # 因为这是短线震荡策略，现实中一定会遇到出场点，所以没有考虑一直未能找到止盈止损的情况
    r = df.loc[pd.notnull(df["signal"])].apply(stopLossProfit)
    
    df['signal'].fillna(method="ffill", inplace=True)
    return df


def getSignalBollingMtm(df, para=[90]):
    n1 = para[0]
    n2 = 35*n1
    df['median'] = df['close'].rolling(window=n2).mean()
    df['std'] = df['close'].rolling(n2, min_periods=1).std(ddof=0)
    df['z_score'] = abs(df['close'] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n2).mean()
    df['upper'] = df['median'] + df['std'] * df['m']
    df['lower'] = df['median'] - df['std'] * df['m']
    condition_long = df['close'] > df['upper']
    condition_short = df['close'] < df['lower']
    df['mtm'] = df['close'] / df['close'].shift(n1) - 1
    df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()
    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
    df['avg_price'] = df['close'].rolling(window=n1, min_periods=1).mean()
    df['wd_atr'] = df['atr'] / df['avg_price']
    df['mtm_l'] = df['low'] / df['low'].shift(n1) - 1
    df['mtm_h'] = df['high'] / df['high'].shift(n1) - 1
    df['mtm_c'] = df['close'] / df['close'].shift(n1) - 1
    df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
    df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()
    df['mtm_l_mean'] = df['mtm_l'].rolling(window=n1, min_periods=1).mean()
    df['mtm_h_mean'] = df['mtm_h'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c_mean'] = df['mtm_c'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
    df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()
    indicator = 'mtm_mean'
    df[indicator] = df[indicator] * df['mtm_atr']
    df[indicator] = df[indicator] * df['mtm_atr_mean']
    df[indicator] = df[indicator] * df['wd_atr']
    df['median'] = df[indicator].rolling(window=n1).mean()
    df['std'] = df[indicator].rolling(n1, min_periods=1).std(ddof=0)
    df['z_score'] = abs(df[indicator] - df['median']) / df['std']
    df['m'] = df['z_score'].rolling(window=n1).min().shift(1)
    df['up'] = df['median'] + df['std'] * df['m']
    df['dn'] = df['median'] - df['std'] * df['m']
    condition1 = df[indicator] > df['up']
    condition2 = df[indicator].shift(1) <= df['up'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_long'] = 1
    condition1 = df[indicator] < df['dn']
    condition2 = df[indicator].shift(1) >= df['dn'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_short'] = -1
    condition1 = df[indicator] < df['median']
    condition2 = df[indicator].shift(1) >= df['median'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_long'] = 0
    condition1 = df[indicator] > df['median']
    condition2 = df[indicator].shift(1) <= df['median'].shift(1)
    condition = condition1 & condition2
    df.loc[condition, 'signal_short'] = 0
    df.loc[condition_long, 'signal_short'] = 0
    df.loc[condition_short, 'signal_long'] = 0
    df.loc[condition_long, 'signal_short'] = 0
    df['signal_long'].fillna(method='ffill', inplace=True)
    df['signal_short'].fillna(method='ffill', inplace=True)
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1, skipna=True)
    df['signal'].fillna(value=0, inplace=True)
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df.drop(['signal_long', 'signal_short', 'atr', 'z_score'], axis=1, inplace=True)
    return df


def getPosition(df):
    
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    df["position"] = df["signal"].shift(1)
    df["position"].fillna(value=0, inplace=True)

    df.sort_values(by="openTimeGmt8", inplace=True)
    df.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)

    return df


# 计算资金曲线
def getEquity(df, para):
    # para:
    # { "cash": 1000,
    #   "faceValue": 0.001,
    #   "commission": 0.04 / 100,
    #   "slippage": 1 / 1000,
    #   "leverage": 3,
    #   "marginMin": 1 / 100,
    # }

    cash = para["cash"]
    faceValue = para["faceValue"]
    commission = para["commission"]
    slippange = para["slippage"]
    leverage = para["leverage"]
    marginMin = para["marginMin"]
    
    # 计算建仓和平仓的时间
    # 找到开仓k线，当前k线非空仓，上一k线与非空仓状态不同，即从另一状态改变成非空仓状态
    condOpen1 = df["position"] != 0
    condOpen2 = df["position"].shift(1) != df["position"]
    condOpen = condOpen1 & condOpen2
    # 找到平仓k线，当前k线为持仓，下一k线与当前持仓状态不同，即从持仓状态改变到另一状态
    condCover1 = df["position"] != 0
    condCover2 = df["position"].shift(-1) != df["position"]
    condCover = condCover1 & condCover2

    # 用开仓时间对每笔交易进行分组，开仓时间即开仓k线的开盘时间，以后就用这个开仓时间来groupby每笔交易
    df.loc[condOpen, "actionTime"] = df["openTimeGmt8"]
    df["actionTime"].fillna(method="ffill", inplace=True)
    df.loc[df["position"]==0, "actionTime"] = pd.NaT
    

    # 在建仓点计算出能购买的合约数量，要计算滑点
    # 滑点的方向根据开仓方向，多仓为正方向，空仓为负方向
    df.loc[condOpen, "openPrice"] = df["open"] * (1 + slippange * df["position"])
    df.loc[condOpen, "contractAmount"] = cash * leverage / (df["open"] * faceValue)
    df["contractAmount"] = np.floor(df["contractAmount"])
    # 本笔交易的开仓资金=初始资金-开仓手续费。每笔交易都单独看待，所以资金数量不重要。最后把每笔交易的收益率（平仓资金-开仓资金）合起来，就是总收益率。
    df["openCash"] = cash - df["contractAmount"] * faceValue * df["openPrice"] * commission
    # 持仓过程中，收益根据收盘价计算，其他数据保持不变
    df["openCash"].fillna(method="ffill", inplace=True)
    df["contractAmount"].fillna(method="ffill", inplace=True)
    df["openPrice"].fillna(method="ffill", inplace=True)
    # 空仓时数据为空
    df.loc[df["position"]==0, ["openCash", "openPrice", "contractAmount"]] = None
    # df.drop(["ma", "upper", "lower"], axis=1, inplace=True)

    # 平仓点上，卖出价按下一根k线开盘价算，扣除手续费和滑点
    df.loc[condCover, "closePrice"] = df["open"].shift(-1) * (1 - slippange * df["position"])
    df.loc[condCover, "fee"] = df["closePrice"] * faceValue * df["contractAmount"] * commission
    
    # 计算收入和净收益，此时还未考虑爆仓情况，爆仓以后还要把净收益归零
    df["profit"] = (df["close"] - df["openPrice"]) * faceValue * df["contractAmount"] * df["position"]
    df.loc[condCover, "profit"] = (df["closePrice"] - df["openPrice"]) * faceValue * df["contractAmount"] * df["position"]
    df["netValue"] = df["openCash"] + df["profit"]
    df.loc[condCover, "netValue"] -= df["fee"]

    # 处理爆仓情况
    # 用k先的最高价最低价，计算出最小净值，用最小净值再计算出当时的保证金率，从而判断是否爆仓
    df.loc[df["position"]==1, "priceMin"] = df["low"]
    df.loc[df["position"]==-1, "priceMin"] = df["high"]
    df["profitMin"] = faceValue * df["contractAmount"] * (df["priceMin"] - df["openPrice"]) * df["position"]
    # 账户净值最小值
    df["netValueMin"] = df["openCash"] + df["profitMin"]
    # 计算最低保证金率
    df["marginRatio"] = df["netValueMin"] / (faceValue * df["contractAmount"] * df["priceMin"])
    # 计算是否爆仓
    df.loc[df["marginRatio"]<=(marginMin + commission), "isFucked"] = 1
    # 按每笔交易处理爆仓，爆仓点以后的netvalue都为0。groupby的fillna要用赋值，不能用inplace=True了
    df["isFucked"] = df.groupby("actionTime")["isFucked"].fillna(method="ffill")
    df.loc[df["isFucked"]==1, "netValue"] = 0

    # 计算资金曲线
    df["equityChange"] = df["netValue"].pct_change()
    df.loc[condOpen, "equityChange"] = df.loc[condOpen, "netValue"] / cash - 1
    df["equityChange"].fillna(value=0, inplace=True)
    df["equityCurve"] = (1 + df["equityChange"]).cumprod()

    # df = df[[
    #     "openTimeGmt8", "open", "high", "low", "close", "volume",
    #     # "ma", "upper", "lower",
    #     "position", "actionTime",
    #     "isFucked", "equityCurve",
    # ]]

    return df


# 发送mixin通知
def sendMixinMsg(msg):
    token = "mrbXSz6rSoQjtrVnDlOH9ogK8UubLdNKClUgx1kGjGoq39usdEzbHlwtFIvHHO3C"
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"
    value = {
        'category':'PLAIN_TEXT', 
        'data':msg,
        }
    try:
        r = requests.post(url, data=value)
        
    except Exception as err:
        print(f"Failed to send mixin message.")
        print(err)
    
