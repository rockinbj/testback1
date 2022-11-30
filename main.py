import datetime as dt
import os
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool

import ccxt
import pandas as pd
from colorama import Fore, init
from tqdm import tqdm

from functions import *
from testConfig import *

pd.set_option("display.expand_frame_repr", False)
pd.set_option("display.max_column", None)
pd.set_option("display.max_rows", 5000)


# 单次计算，受python多线程传参限制，第一个参数布林参数可变，后面都是固定参数
def sigalTest(paraBolling, df, paraTrading, equityFilePath):
    _df = df.copy()
    levelTest = paraBolling[0]
    maLength = paraBolling[1]
    times = paraBolling[2]

    # 合并k线
    _df = rebuildCandles(_df, levelTest)

    # 计算实际持仓
    _df = getBollingPosition(_df, [maLength, times])

    # 计算资金曲线
    # 选取相关时间。币种上线10天之后的日期
    # t = _df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
    # _df = _df[_df['candle_begin_time'] > t]
    # 计算资金曲线
    _df = getBollingEquity(_df, paraTrading)

    # 只存储满足期望盈亏比的数据，如果开了3倍杠杆，盈利倍数1.5，就只存储收益率大于4.5的数据
    # if _df.iloc[-1]["equityCurve"] > (paraTrading["leverage"] * plRate):
    #     equityFileName = f'{levelTest}_{maLength}_{times}_{round(_df.iloc[-1]["equityCurve"], 3)}.{equityFileFmt}'
    #     equityFile = os.path.join(equityFilePath, equityFileName)
    #     os.makedirs(equityFilePath, exist_ok=True)
    #     if equityFileFmt == "csv":
    #         _df.to_csv(equityFile, index=False)
    #     elif equityFileFmt == "hdf":
    #         _df.to_hdf(equityFile, index=False, mode="w", complevel=5, key="_df")

    # 计算收益指标：布林参数（testLevel周期、maLength均线长度、times倍数），
    # finalEquity最终收益率，leverage杠杆倍数，isFucked爆仓次数，totalTrades交易次数，winRate胜率
    rtn = pd.DataFrame()
    rtn.loc[0, "testLevel"] = levelTest
    rtn.loc[0, "maLength"] = maLength
    rtn.loc[0, "times"] = times
    rtn.loc[0, 'finalEquity'] = round(_df.iloc[-1]['equityCurve'], 3)
    rtn.loc[0, "leverage"] = paraTrading["leverage"]
    rtn.loc[0, "isFucked"] = len(_df.loc[_df["isFucked"]==1])
    totalTrades = _df["actionTime"].nunique()
    rtn.loc[0, "totalTrades"] = totalTrades
    win = []
    g1 = _df.groupby("actionTime")
    for key, group in g1:
        if group.iloc[-1]["equityCurve"] - group.iloc[0]["equityCurve"] > 0:
            win.append(key)
    totalWins = len(win)
    if totalTrades != 0:
        rtn.loc[0, "winRate"] = round(totalWins/totalTrades, 2)
    else:
        rtn.loc[0, "winRate"] = 0
    
    return rtn

def main(equityFilePath):
    init()  # 彩色字体初始化
    # 生成布林带所有参数组合
    paraBollings = getBollingParas(levelList, maLengthList, timesList)
    ex = ccxt.binance()

    # 如果没找到原始数据文件，就开始下载
    if os.path.exists(dataFile) is False:
        print(Fore.RED+"Data file doesn0t exists, downloading..."+Fore.RESET)
        df = getRecords(ex, symbol, levelBase, startTimeData, endTimeData)
        writeRecordsToFile(df, dataFile, dataFileFmt)
    # 如果有原始数据文件，就读取，并且只使用“使用数据起止时间”内的数据
    else:
        print(Fore.GREEN+f"Loading data file:{Fore.RESET} {dataFile}")
        if dataFileFmt == "hdf":
            df = pd.read_hdf(dataFile, parse_dates=["openTimeGmt8"])
        
        elif dataFileFmt == "csv":
            df = pd.read_csv(dataFile, parse_dates=["openTimeGmt8"])
        
        df = df[(df["openTimeGmt8"]>=pd.to_datetime(startTimeUse)) & (df["openTimeGmt8"]<=pd.to_datetime(endTimeUse))]

    # 把固定参数先传进去，然后把布林参数作为可变参数放进线程
    processMax = min(len(paraBollings), cpu_count())
    print(f"Opening {Fore.MAGENTA+str(processMax)} {Fore.RESET}processes...")
    # processMax = 1
    callback = partial(sigalTest, df=df, paraTrading=paraTrading, equityFilePath=equityFilePath)
    with Pool(processes=processMax) as pool:
        dfList = pool.map(callback, tqdm(paraBollings, ncols=100))
        final = pd.concat(dfList, ignore_index=True)
        final.sort_values("finalEquity", ascending=False, inplace=True)
        reportFile = os.path.join(equityFilePath, f"report_{t}.csv")
        final.to_csv(reportFile, index=False)
        sendMixinMsg(f"Testback successfully:\n {strategy} {symbol}\n {startTimeUse}-{endTimeUse}\n total Paras: {len(paraBollings)}\n best Equity: {final['finalEquity'].iat[0]}\n best para:\n {final[['testLevel', 'maLength', 'times']].iloc[0]}")


if __name__ == "__main__":
    t = str(dt.datetime.now()).replace("-","").replace(" ","").replace(":", "")[:14]
    # equityFilePath = f'dataStore\\{strategy}_equity\\{symbol.replace("/","-")}\\{t}'
    equityFilePath = os.path.join("dataStore", f"{strategy}_equity", symbol.replace("/","-"), t)
    os.makedirs(equityFilePath, exist_ok=True)
    main(equityFilePath)
