import datetime as dt
import os
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from itertools import groupby

import ccxt
import pandas as pd
from colorama import Fore, init
from tqdm import tqdm

from functions import *
from testConfig import *

pd.set_option("display.expand_frame_repr", False)
pd.set_option("display.max_column", None)
# pd.set_option("display.max_rows", 5000)
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)


# 单次计算，受python多线程传参限制，第一个参数布林参数可变，后面都是固定参数
def sigalTest(paraBolling, df, paraTrading, equityFilePath):
    # print(f"paras: {paraBolling}")
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
    # # if _df.iloc[-1]["equityCurve"] > 0:
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
    rtn.loc[0, '最终净值'] = round(_df.iloc[-1]['equityCurve'], 3)
    rtn.loc[0, "杠杆倍数"] = paraTrading["leverage"]
    rtn.loc[0, "是否爆仓"] = len(_df.loc[_df["isFucked"]==1])
    totalTrades = _df["actionTime"].nunique()
    rtn.loc[0, "交易次数"] = totalTrades
    rtn.loc[0, "平均每单盈利"] = round((rtn.at[0, '最终净值']/totalTrades),4) if totalTrades!=0 else 0

    win = []
    g1 = _df.groupby("actionTime")
    _temp = pd.DataFrame()  # 存储每笔的统计数据
    
    # 存在一次交易都没有产生的情况，actionTime为空，则g1为空，则_temp为空，程序报错
    # 用if规避一下
    if g1:
        for key, group in g1:
            equityLast = group.iloc[-1]["equityCurve"]
            equityFirst = group.iloc[0]["equityCurve"]
            timeLast = group.iloc[-1]["openTimeGmt8"]
            timeFirst = group.iloc[0]["openTimeGmt8"]
            if equityLast - equityFirst > 0:
                win.append(key)
            _temp.at[key, "profitRate"] = (equityLast/equityFirst-1) if equityFirst else 0
            _temp.at[key, "forHours"] = float((timeLast-timeFirst).seconds / 60 / 60)
    else:
        _temp["profitRate"] = 0
        _temp["forHours"] = 0

    # 计算连续盈利次数和连续亏损次数，将是否连续的辅助列用itertools.groupby运算得到连续次数
    _temp["profitRate"].fillna(0)
    _temp["isContinual"] = 0
    _temp.loc[(_temp["profitRate"]>0)&(_temp["profitRate"].shift()>0), "isContinual"] = 1
    _temp.loc[(_temp["profitRate"]<0)&(_temp["profitRate"].shift()<0), "isContinual"] = -1

    _tempGroup = groupby(_temp["isContinual"].values.tolist())
    _t1, _t2 = [], []  # 为正的次数和为负的次数，最后用max找最大
    for k,v in _tempGroup:
        if k==1:
            _t1.append(len(list(v)))
        elif k==-1:
            _t2.append(len(list(v)))
    
    rtn.loc[0, "最大连续盈利次数"] = max(_t1) + 1 if _t1 else 0
    rtn.loc[0, "最大连续亏损次数"] = max(_t2) + 1 if _t2 else 0

    rtn.loc[0, "最长持仓时间"] = round(_temp["forHours"].max(),1)
    rtn.loc[0, "最短持仓时间"] = round(_temp.loc[_temp["forHours"]>0, "forHours"].min(),1)
    rtn.loc[0, "平均持仓时间"] = round(_temp["forHours"].sum()/totalTrades,1) if totalTrades else 0
        
    totalWins = len(win)
    rtn.loc[0, "胜率"] = round(totalWins/totalTrades, 2) if totalTrades else 0

    
    # 计算回撤，cummax是前期高点，最大回撤率=(当前资金曲线值 - 前期高点) / 前期高点
    _df["equityPreMax"] = _df["equityCurve"].cummax()
    _df["drawDown"] = _df["equityCurve"] / _df["equityPreMax"] - 1

    # 找到最大回撤和对应时间
    drawdownMax = round(_df["drawDown"].min(), 4)
    drawdownMaxTime = _df.set_index("openTimeGmt8")["drawDown"].idxmin()
    rtn.loc[0, "最大回撤"] = drawdownMax
    rtn.loc[0, "最大回撤发生时间"] = drawdownMaxTime

    # 计算年化收益
    eqFirst = _df["equityCurve"].iat[0]
    eqLast = _df["equityCurve"].iat[-1]
    days = max((_df["openTimeGmt8"].iat[-1] - _df["openTimeGmt8"].iat[0]).days, 1)
    # print(eqFirst, eqLast, days)
    ar = pow((eqLast-eqFirst), (250/days)) -1 if eqLast>eqFirst else 0
    rtn.loc[0, "年化收益"] = round(ar, 4)
    
    # 收益回撤比、收益风险比 return/risk rate
    rtn.loc[0, "收益风险比(年化/回撤)"] = abs(round(ar/drawdownMax, 4)) if drawdownMax else 999999

    rtn = rtn[[
        "testLevel", "maLength", "times",
        "最终净值", "年化收益", "最大回撤", "收益风险比(年化/回撤)",
        "交易次数", "胜率", "最大连续盈利次数", "最大连续亏损次数", "平均每单盈利",
        "最长持仓时间", "最短持仓时间", "平均持仓时间", 
        "杠杆倍数", "是否爆仓","最大回撤发生时间",
    ]]
    # print(f"_df:\n {_df}")
    # print(f"_temp:\n{_temp}")
    # print(f"rtn:\n{rtn}")
    return rtn

def main(equityFilePath):
    init()  # 彩色字体初始化
    # 生成布林带所有参数组合
    paraBollings = getBollingParas(levelList, maLengthList, timesList)
    ex = ccxt.binance({
        "options":{
            "defaultType":"future",
        },
        "timeout": 1000,
    })

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
        final.sort_values("最终净值", ascending=False, inplace=True)
        reportFile = os.path.join(equityFilePath, f"report_{t}.csv")
        final.to_csv(reportFile, index=False, encoding="GBK")
        sendMixinMsg(f"Testback successfully:\n {strategy} {symbol}\n {startTimeUse}-{endTimeUse}\n total Paras: {len(paraBollings)}\n best Equity: {final['最终净值'].iat[0]}\n best para:\n {final[['testLevel', 'maLength', 'times']].iloc[0]}")


if __name__ == "__main__":
    t = str(dt.datetime.now()).replace("-","").replace(" ","").replace(":", "")[:14]
    # equityFilePath = f'dataStore\\{strategy}_equity\\{symbol.replace("/","-")}\\{t}'
    equityFilePath = os.path.join("dataStore", f"{strategy}_equity", symbol.replace("/","-"), t)
    os.makedirs(equityFilePath, exist_ok=True)
    main(equityFilePath)
