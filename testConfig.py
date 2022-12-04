import os

## 所有参数设置
SYMBOL = "ETH/USDT"
STRATEGY = "Sma3"

# 布林带策略参数
PARA_LEVEL_LIST = ["4h"]
PARA_MA_LIST=[180]
PARA_TIMES_LIST = [2.9]
PARA_PERCENT_LIST = [10]
# PARA_LEVEL_LIST = ["15m", "30m", "1h"]
# PARA_MA_LIST = range(10, 1000, 5)
# PARA_TIMES_LIST = [i/10 for i in range(10, 40)]
# PARA_PERCENT_LIST = range(10, 50, 10)

# 均线策略参数
LEVEL_LIST = ["15m", "30m", "1h", "4h", "1d"]
SMA1_LIST = [5, 10, 15, 20, 25, 30]
SMA2_LIST = [50, 55, 60, 65, 70]
SMA3_LIST = [100, 110, 120, 150, 200]
DIST_LIST = range(3, 50, 5)
# LEVEL_LIST = ["4h"]
# SMA1_LIST = [30]
# SMA2_LIST = [70]
# SMA3_LIST = [200]
# DIST_LIST = [3]

# 生成参数列表
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_MA_LIST, PARA_TIMES_LIST]
# PARAS_LIST = [PARA_LEVEL_LIST, PARA_MA_LIST, PARA_TIMES_LIST, PARA_PERCENT_LIST]
PARAS_LIST = [LEVEL_LIST, SMA1_LIST, SMA2_LIST, SMA3_LIST, DIST_LIST]


# 测试数据的起止时间
START_TIME_TEST = "2020-10-01 00:00:00"
END_TIME_TEST = "2022-12-03 00:00:00"

# 单个参数测试结果的格式
SINGAL_TEST_FORMAT = "csv"

# 期望盈利目标（倍数），不满足该倍数的结果不保存，0不限制全保存
PL_RATE = 0

# 原始数据的k线级别
LEVEL = "5m"
# 原始数据的起止时间
START_TIME_DATA = "2020-10-01 00:00:00"
END_TIME_DATA = "2022-12-03 00:00:00"
# 原始数据文件的格式和命名
DATA_FILE_FORMAT = "hdf"
# DATA_FILE = os.path.join("dataStore", "data_ETH-USDT_1m_20180101_20221110.hdf")
DATA_FILE = os.path.join("dataStore", f'data_{SYMBOL.replace("/","-")}_{LEVEL}_{START_TIME_TEST.replace("-","").replace(" ","")[:8]}_{END_TIME_TEST.replace("-","").replace(" ","")[:8]}.{DATA_FILE_FORMAT}')

# 交易所参数
EXCHANGE_CONFIG = {
    "options":{
        "defaultType":"future",
    },
    "timeout": 2000,
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
