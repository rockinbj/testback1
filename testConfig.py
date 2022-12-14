import os


## 所有参数设置
SYMBOL = "DOGE/USDT"
STRATEGY = "Psy"


# 测试数据的起止时间
START_TIME_TEST = "2020-01-10 00:00:00"
END_TIME_TEST = "2022-12-12 00:00:00"
DATA_FILE_NAME = "data_DOGE-USDT_swap_5m_2020-07-10_2022-12-12.hdf"

# 所有策略公用的周期，必须放在PARAS_LIST的第一个
PARA_LEVEL_LIST = ["30m", "1h"]


# PSY参数
PARA_PSY_N_LIST = range(10,500,10)
PARA_PSY_M_LIST = range(1,50,5)
PARA_PSY_STOPLOSS_LIST = range(1,30,5)
PARAS_LIST = [PARA_LEVEL_LIST, PARA_PSY_N_LIST, PARA_PSY_M_LIST, PARA_PSY_STOPLOSS_LIST]


# 动量布林BollingMtm参数
# PARA_TIMES_LIST = [90]
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_TIMES_LIST]


# 布林延迟BollingDelay参数
# PARA_MA_LIST=[180]
# PARA_TIMES_LIST = [2.9]
# PARA_PERCENT_LIST = [10]
# PARA_LEVEL_LIST = ["15m", "30m", "1h"]
# PARA_MA_LIST = range(10, 1000, 10)
# PARA_TIMES_LIST = [i/10 for i in range(10, 40)]
# PARA_PERCENT_LIST = range(3, 50, 5)
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_MA_LIST, PARA_TIMES_LIST, PARA_PERCENT_LIST]


# 三均线Sma3参数
# SMA1_LIST = [5, 10, 15, 20, 25, 30]
# SMA2_LIST = [50, 55, 60, 65, 70]
# SMA3_LIST = [90, 95, 100, 110, 120, 150, 200, 250, 300]
# DIST_LIST = range(1, 20, 2)  # 短均线长均线距离1%~20%,步长2%
# LEVEL_LIST = ["4h"]
# SMA1_LIST = [20]
# SMA2_LIST = [60]
# SMA3_LIST = [120]
# DIST_LIST = [3]
# PARAS_LIST = [PARA_LEVEL_LIST, SMA1_LIST, SMA2_LIST, SMA3_LIST, DIST_LIST]


# Nwe参数
# NWE_LEN_LIST = [500]
# NWE_BAND_LIST = [8]
# NWE_TIMES_LIST = [3]
# ATR_LEN_LIST = [14]
# ATR_TIMES_LIST = [0.5, 1]
# RSI_LEN_LIST = [5]
# NWE_PLRATE_LIST = [1.5, 2]  # 目标盈亏比
# PARAS_LIST = [PARA_LEVEL_LIST, NWE_LEN_LIST, NWE_BAND_LIST, NWE_TIMES_LIST, ATR_LEN_LIST, ATR_TIMES_LIST, RSI_LEN_LIST, NWE_PLRATE_LIST]



# 保存回测结果文件时的盈利目标（倍数）限制，不满足该倍数的结果不保存，0不限制全保存
PL_RATE = 0


# 原始数据的k线级别
LEVEL = "5m"
# 原始数据的起止时间
START_TIME_DATA = "2017-08-17 00:00:00"
END_TIME_DATA = "2022-12-12 00:00:00"
# 原始数据文件的格式和命名
DATA_FILE_FORMAT = "hdf"
DATA_FILE = os.path.join("dataStore", DATA_FILE_NAME)
# DATA_FILE = os.path.join("dataStore", f'data_{SYMBOL.replace("/","-")}_{LEVEL}_{START_TIME_DATA.replace("-","").replace(" ","")[:8]}_{END_TIME_DATA.replace("-","").replace(" ","")[:8]}.{DATA_FILE_FORMAT}')


# 交易所参数
EXCHANGE_CONFIG = {
    "options":{
        # "defaultType":"future",
    },
    "timeout": 5000,
}


# 交易参数
PARA_TRADING = {
    "cash": 10000,  # 初始现金、每次交易金额
    "faceValue": 0.001,  # 单张合约面值
    "commission": 2 / 1000,  # 手续费
    "slippage": 1 / 1000,  # 滑点
    "leverage": 3,  # 杠杆
    "marginMin": 15 / 100,  # 最低保障金率，低于爆仓
}


# 睡眠时间
SLEEP_SHORT = 0.2
SLEEP_MEDIUM = 1
SLEEP_LONG = 5


# 单个参数测试结果的格式
SINGAL_TEST_FORMAT = "csv"