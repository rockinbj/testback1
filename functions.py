import datetime as dt
import time

import numpy as np
import pandas as pd
import requests

sleepShort = 0.3
sleepMedium = 3
sleepLong = 9

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
        time.sleep(sleepShort)
    
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


# 计算持仓情况
def getBollingPosition(dataFrame, para):
    # para:
    # [maLength, times]
    # [400, 2]
    
    maLength = para[0]
    times = para[1]

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    dataFrame["ma"] = dataFrame["close"].rolling(maLength).mean()
    dataFrame["stdDev"] = dataFrame["close"].rolling(maLength).std(ddof=0)
    dataFrame["upper"] = dataFrame["ma"] + times * dataFrame["stdDev"]
    dataFrame["lower"] = dataFrame["ma"] - times * dataFrame["stdDev"]

    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = dataFrame["close"].shift(1) <= dataFrame["upper"].shift(1)
    condLong2 = dataFrame["close"] > dataFrame["upper"]
    dataFrame.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = dataFrame["close"].shift(1) >= dataFrame["ma"].shift(1)
    condCoverLong2 = dataFrame["close"] < dataFrame["ma"]
    dataFrame.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = dataFrame["close"].shift(1) >= dataFrame["lower"].shift(1)
    condShort2 = dataFrame["close"] < dataFrame["lower"]
    dataFrame.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = dataFrame["close"].shift(1) <= dataFrame["ma"].shift(1)
    condCoverShort2 = dataFrame["close"] > dataFrame["ma"]
    dataFrame.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    # 填充signal的空白
    # dataFrame["signal"].fillna(method="ffill", inplace=True)
    # dataFrame["signal"].fillna(value=0, inplace=True)
    dataFrame["signal"] = dataFrame[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = dataFrame[dataFrame["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    dataFrame["signal"] = temp["signal"]
    dataFrame['signal'].fillna(method='ffill', inplace=True)
    dataFrame['signal'].fillna(value=0, inplace=True)
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    dataFrame["position"] = dataFrame["signal"].shift(1)
    dataFrame["position"].fillna(value=0, inplace=True)
    
    dataFrame.drop(["stdDev", "signal"], axis=1, inplace=True)
    dataFrame.sort_values(by="openTimeGmt8", inplace=True)
    dataFrame.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)
    
    return dataFrame


# 优化后的布林带策略，考虑开仓点位与中轨距离
def getBollingPositionOptimized(dataFrame, para=[400, 2, 0.03]):
    # para:
    # [maLength, times, n]
    # [400, 2, 0.03]
    # 当出现开仓信号，但是此时开仓信号距离中轨距离超过n%，则保持开仓信号，等到距离下降至n%以下再开仓
    # 如果中间出现其他信号，则放弃保持的开仓信号

    
    maLength = para[0]
    times = para[1]

    # 计算布林带上轨(upper)、中轨(ma)、下轨(lower)
    dataFrame["ma"] = dataFrame["close"].rolling(maLength).mean()
    dataFrame["stdDev"] = dataFrame["close"].rolling(maLength).std(ddof=0)
    dataFrame["upper"] = dataFrame["ma"] + times * dataFrame["stdDev"]
    dataFrame["lower"] = dataFrame["ma"] - times * dataFrame["stdDev"]

    # 计算价格与中轨距离
    dataFrame["dist"] = abs(dataFrame["close"] / dataFrame["ma"] - 1)
    print(dataFrame.tail(50))
    raise


    # 计算开多(收盘价上穿上轨，signal=1)、平多(收盘价下穿中轨，signal=0)
    condLong1 = dataFrame["close"].shift(1) <= dataFrame["upper"].shift(1)
    condLong2 = dataFrame["close"] > dataFrame["upper"]
    dataFrame.loc[condLong1 & condLong2, "signalLong"] = 1
    
    condCoverLong1 = dataFrame["close"].shift(1) >= dataFrame["ma"].shift(1)
    condCoverLong2 = dataFrame["close"] < dataFrame["ma"]
    dataFrame.loc[condCoverLong1 & condCoverLong2, "signalLong"] = 0

    # 计算开空(收盘价下穿下轨，signal=-1)、平空(收盘价上穿中轨，signal=0)
    condShort1 = dataFrame["close"].shift(1) >= dataFrame["lower"].shift(1)
    condShort2 = dataFrame["close"] < dataFrame["lower"]
    dataFrame.loc[condShort1 & condShort2, "signalShort"] = -1

    condCoverShort1 = dataFrame["close"].shift(1) <= dataFrame["ma"].shift(1)
    condCoverShort2 = dataFrame["close"] > dataFrame["ma"]
    dataFrame.loc[condCoverShort1 & condCoverShort2, "signalShort"] = 0

    # 填充signal的空白
    # dataFrame["signal"].fillna(method="ffill", inplace=True)
    # dataFrame["signal"].fillna(value=0, inplace=True)
    dataFrame["signal"] = dataFrame[["signalLong", "signalShort"]].sum(axis=1, min_count=1, skipna=True)
    temp = dataFrame[dataFrame["signal"].notnull()][["signal"]]
    temp = temp[temp["signal"] != temp["signal"].shift(1)]
    dataFrame["signal"] = temp["signal"]
    dataFrame['signal'].fillna(method='ffill', inplace=True)
    dataFrame['signal'].fillna(value=0, inplace=True)
    # 计算持仓，在产生signal信号的k线结束时进行买入，因此持仓状态比signal信号k线晚一根k线
    dataFrame["position"] = dataFrame["signal"].shift(1)
    dataFrame["position"].fillna(value=0, inplace=True)
    
    dataFrame.drop(["stdDev", "signal"], axis=1, inplace=True)
    dataFrame.sort_values(by="openTimeGmt8", inplace=True)
    dataFrame.drop_duplicates(subset="openTimeGmt8", keep="last", inplace=True)
    
    return dataFrame


# 计算资金曲线
def getBollingEquity(dataFrame, para):
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
    condOpen1 = dataFrame["position"] != 0
    condOpen2 = dataFrame["position"].shift(1) != dataFrame["position"]
    condOpen = condOpen1 & condOpen2
    # 找到平仓k线，当前k线为持仓，下一k线与当前持仓状态不同，即从持仓状态改变到另一状态
    condCover1 = dataFrame["position"] != 0
    condCover2 = dataFrame["position"].shift(-1) != dataFrame["position"]
    condCover = condCover1 & condCover2

    # 用开仓时间对每笔交易进行分组，开仓时间即开仓k线的开盘时间，以后就用这个开仓时间来groupby每笔交易
    dataFrame.loc[condOpen, "actionTime"] = dataFrame["openTimeGmt8"]
    dataFrame["actionTime"].fillna(method="ffill", inplace=True)
    dataFrame.loc[dataFrame["position"]==0, "actionTime"] = pd.NaT
    

    # 在建仓点计算出能购买的合约数量，要计算滑点
    # 滑点的方向根据开仓方向，多仓为正方向，空仓为负方向
    dataFrame.loc[condOpen, "openPrice"] = dataFrame["open"] * (1 + slippange * dataFrame["position"])
    dataFrame.loc[condOpen, "contractAmount"] = cash * leverage / (dataFrame["open"] * faceValue)
    dataFrame["contractAmount"] = np.floor(dataFrame["contractAmount"])
    # 本笔交易的开仓资金=初始资金-开仓手续费。每笔交易都单独看待，所以资金数量不重要。最后把每笔交易的收益率（平仓资金-开仓资金）合起来，就是总收益率。
    dataFrame["openCash"] = cash - dataFrame["contractAmount"] * faceValue * dataFrame["openPrice"] * commission
    # 持仓过程中，收益根据收盘价计算，其他数据保持不变
    dataFrame["openCash"].fillna(method="ffill", inplace=True)
    dataFrame["contractAmount"].fillna(method="ffill", inplace=True)
    dataFrame["openPrice"].fillna(method="ffill", inplace=True)
    # 空仓时数据为空
    dataFrame.loc[dataFrame["position"]==0, ["openCash", "openPrice", "contractAmount"]] = None
    # dataFrame.drop(["ma", "upper", "lower"], axis=1, inplace=True)

    # 平仓点上，卖出价按下一根k线开盘价算，扣除手续费和滑点
    dataFrame.loc[condCover, "closePrice"] = dataFrame["open"].shift(-1) * (1 - slippange * dataFrame["position"])
    dataFrame.loc[condCover, "fee"] = dataFrame["closePrice"] * faceValue * dataFrame["contractAmount"] * commission
    
    # 计算收入和净收益，此时还未考虑爆仓情况，爆仓以后还要把净收益归零
    dataFrame["profit"] = (dataFrame["close"] - dataFrame["openPrice"]) * faceValue * dataFrame["contractAmount"] * dataFrame["position"]
    dataFrame.loc[condCover, "profit"] = (dataFrame["closePrice"] - dataFrame["openPrice"]) * faceValue * dataFrame["contractAmount"] * dataFrame["position"]
    dataFrame["netValue"] = dataFrame["openCash"] + dataFrame["profit"]
    dataFrame.loc[condCover, "netValue"] -= dataFrame["fee"]

    # 处理爆仓情况
    # 用k先的最高价最低价，计算出最小净值，用最小净值再计算出当时的保证金率，从而判断是否爆仓
    dataFrame.loc[dataFrame["position"]==1, "priceMin"] = dataFrame["low"]
    dataFrame.loc[dataFrame["position"]==-1, "priceMin"] = dataFrame["high"]
    dataFrame["profitMin"] = faceValue * dataFrame["contractAmount"] * (dataFrame["priceMin"] - dataFrame["openPrice"]) * dataFrame["position"]
    # 账户净值最小值
    dataFrame["netValueMin"] = dataFrame["openCash"] + dataFrame["profitMin"]
    # 计算最低保证金率
    dataFrame["marginRatio"] = dataFrame["netValueMin"] / (faceValue * dataFrame["contractAmount"] * dataFrame["priceMin"])
    # 计算是否爆仓
    dataFrame.loc[dataFrame["marginRatio"]<=(marginMin + commission), "isFucked"] = 1
    # 按每笔交易处理爆仓，爆仓点以后的netvalue都为0。groupby的fillna要用赋值，不能用inplace=True了
    dataFrame["isFucked"] = dataFrame.groupby("actionTime")["isFucked"].fillna(method="ffill")
    dataFrame.loc[dataFrame["isFucked"]==1, "netValue"] = 0

    # 计算资金曲线
    dataFrame["equityChange"] = dataFrame["netValue"].pct_change()
    dataFrame.loc[condOpen, "equityChange"] = dataFrame.loc[condOpen, "netValue"] / cash - 1
    dataFrame["equityChange"].fillna(value=0, inplace=True)
    dataFrame["equityCurve"] = (1 + dataFrame["equityChange"]).cumprod()

    # dataFrame.drop(
    #     [
    #         "openPrice", "contractAmount", "openCash", 
    #         "profit", "netValue", "closePrice", "fee",
    #         "priceMin", "profitMin", "netValueMin", "marginRatio",
    #     ], axis=1, inplace=True)

    return dataFrame


# 生成策略参数组合
def getBollingParas(levelList, maLengthList, timesList):
    """
    产生布林 策略的参数范围
    :param levelList: k线周期
    :param maLengthList: 中轨长度
    :param timesList: 倍数
    :return:
    """
    para_list = []

    for l in levelList:
        for m in maLengthList:
            for n in timesList:
                para = [l, m, n]
                para_list.append(para)

    return para_list


# 生成优化布林带的策略参数组合
def getBollingParasOptimized(levelList, maLengthList, timesList, distList):
    """
    产生布林 策略的参数范围
    :param levelList: k线周期
    :param maLengthList: 中轨长度
    :param timesList: 倍数
    :return:
    """
    para_list = []

    for l in levelList:
        for m in maLengthList:
            for n in timesList:
                for o in distList:
                    para = [l, m, n, o]
                    para_list.append(para)

    return para_list


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
    
