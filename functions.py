import datetime as dt
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
def getPositionBolling(df, para):
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

    # 填充signal的空白
    # df["signal"].fillna(method="ffill", inplace=True)
    # df["signal"].fillna(value=0, inplace=True)
    df["signal"] = df[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = df[df["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    df["signal"] = temp["signal"]
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    df["position"] = df["signal"].shift(1)
    df["position"].fillna(value=0, inplace=True)
    
    df.drop(["stdDev", "signal"], axis=1, inplace=True)
    df.sort_values(by="openTimeGmt8", inplace=True)
    df.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)
    
    return df


# 平均差布林策略信号计算
def getPositionBollingMean(df, para):
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
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    df["position"] = df["signal"].shift(1)
    df["position"].fillna(value=0, inplace=True)
    
    df.drop(["diff", "signal"], axis=1, inplace=True)
    df.sort_values(by="openTimeGmt8", inplace=True)
    df.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)
    
    return df


# 布林增加延迟开仓，用的pipe，较慢
def getPositionBollingDelay2(df, para):
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
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    df["position"] = df["signal"].shift(1)
    df["position"].fillna(value=0, inplace=True)
    
    df.drop(["stdDev", "signal"], axis=1, inplace=True)
    df.sort_values(by="openTimeGmt8", inplace=True)
    df.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)

    return df


# 布林增加延迟开仓
def getPositionBollingDelay(df, para):
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
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    df["position"] = df["signal"].shift(1)
    df["position"].fillna(value=0, inplace=True)
    
    df.drop(["stdDev", "signal"], axis=1, inplace=True)
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

    df = df[[
        "openTimeGmt8", "open", "high", "low", "close", "volume",
        "ma", "upper", "lower",
        "position", "actionTime",
        "isFucked", "equityCurve",
    ]]

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
    
