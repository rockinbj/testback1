import os

## 所有参数设置
SYMBOL = "ETH/USDT"
STRATEGY = "BollingMtm"

# 动量布林BollingMtm参数
PARA_LEVEL_LIST = ["5m", "15m", "30m", "1h", "4h", "1d"]
PARA_TIMES_LIST = range(1,100)

# 简单布林Bolling参数
# PARA_LEVEL_LIST = ["4h"]
# PARA_MA_LIST=[180]
# PARA_TIMES_LIST = [2.9]
# PARA_PERCENT_LIST = [10]
# PARA_LEVEL_LIST = ["15m", "30m", "1h"]
# PARA_MA_LIST = range(10, 1000, 10)
# PARA_TIMES_LIST = [i/10 for i in range(10, 40)]
# PARA_PERCENT_LIST = range(3, 50, 5)

# 三均线Sma3参数
# LEVEL_LIST = ["15m", "30m", "1h", "4h", "1d"]
# SMA1_LIST = [5, 10, 15, 20, 25, 30]
# SMA2_LIST = [50, 55, 60, 65, 70]
# SMA3_LIST = [90, 95, 100, 110, 120, 150, 200, 250, 300]
# DIST_LIST = range(1, 20, 2)  # 短均线长均线距离1%~20%,步长2%
# LEVEL_LIST = ["4h"]
# SMA1_LIST = [20]
# SMA2_LIST = [60]
# SMA3_LIST = [120]
# DIST_LIST = [3]

# Nwe参数
# LEVEL_LIST = ["1m", "5m", "15m", "30m"]
# NWE_LEN_LIST = [500]
# NWE_BAND_LIST = [8]
# NWE_TIMES_LIST = [3]
# ATR_LEN_LIST = [14]
# ATR_TIMES_LIST = [0.5, 1]
# RSI_LEN_LIST = [5]
# NWE_PLRATE_LIST = [1.5, 2]  # 目标盈亏比

# 生成参数列表
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_MA_LIST, PARA_TIMES_LIST]
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_MA_LIST, PARA_TIMES_LIST, PARA_PERCENT_LIST]
# PARAS_LIST = [LEVEL_LIST, SMA1_LIST, SMA2_LIST, SMA3_LIST, DIST_LIST]
# PARAS_LIST = [LEVEL_LIST, NWE_LEN_LIST, NWE_BAND_LIST, NWE_TIMES_LIST, ATR_LEN_LIST, ATR_TIMES_LIST, RSI_LEN_LIST, NWE_PLRATE_LIST]
PARAS_LIST = [PARA_LEVEL_LIST, PARA_TIMES_LIST]


# 测试数据的起止时间
START_TIME_TEST = "2018-10-01 00:00:00"
END_TIME_TEST = "2022-12-03 00:00:00"

# 单个参数测试结果的格式
SINGAL_TEST_FORMAT = "csv"

# 保存回测结果文件时的盈利目标（倍数）限制，不满足该倍数的结果不保存，0不限制全保存
PL_RATE = 0

# 原始数据的k线级别
LEVEL = "5m"
# 原始数据的起止时间
START_TIME_DATA = "2018-12-01 00:00:00"
END_TIME_DATA = "2022-12-05 00:00:00"
# 原始数据文件的格式和命名
DATA_FILE_FORMAT = "hdf"
# DATA_FILE = os.path.join("dataStore", "data_BTC-USDT_5m_20180701_20221124.hdf")
DATA_FILE = os.path.join("dataStore", f'data_{SYMBOL.replace("/","-")}_{LEVEL}_{START_TIME_DATA.replace("-","").replace(" ","")[:8]}_{END_TIME_DATA.replace("-","").replace(" ","")[:8]}.{DATA_FILE_FORMAT}')

# 交易所参数
EXCHANGE_CONFIG = {
    "options":{
        "defaultType":"future",
    },
    "timeout": 5000,
}

# 交易参数
PARA_TRADING = {
    "cash": 10000,  # 初始现金、每次交易金额
    "faceValue": 0.001,  # 单张合约面值
    "commission": 4 / 10000,  # 手续费
    "slippage": 1 / 1000,  # 滑点
    "leverage": 3,  # 杠杆
    "marginMin": 1 / 100,  # 最低保障金率，低于爆仓
}

# 睡眠时间
SLEEP_SHORT = 0.2
SLEEP_MEDIUM = 1
SLEEP_LONG = 5
