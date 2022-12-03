import os

## 所有参数设置
SYMBOL = "ETH/USDT"
STRATEGY = "BollingMean"

# 期望盈利目标（倍数），不满足该倍数的结果不保存
PL_RATE = 3
# 原始数据
# 原始数据的k线级别
LEVEL = "5m"
# 原始数据的起止时间
START_TIME_DATA = "2020-10-01 00:00:00"
END_TIME_DATA = "2022-12-01 00:00:00"
# 原始数据文件的格式和命名
DATA_FILE_FORMAT = "hdf"
DATA_FILE = os.path.join("dataStore", "data_ETH-USDT_1m_20180101_20221110.hdf")
# dataFile = os.path.join("dataStore", f'data_{symbol.replace("/","-")}_{levelBase}_{startTimeData.replace("-","").replace(" ","")[:8]}_{endTimeData.replace("-","").replace(" ","")[:8]}.{dataFileFmt}')

# 测试数据（原始数据中的一部分）
# 测试数据的起止时间
START_TIME_TEST = "2020-10-01 00:00:00"
END_TIME_TEST = "2022-12-01 00:00:00"
# 生成布林带测试参数组合
PARA_LEVEL_LIST = ["4h"]
PARA_MA_LIST=[180]
PARA_TIMES_LIST = [2.9]
# PARA_LEVEL_LIST = ["5m", "15m", "30m", "1h", "4h", "1d"]
# PARA_MA_LIST = range(10, 2000, 5)
# PARA_TIMES_LIST = [i/10 for i in range(5, 50, 1)]

# 单个参数测试结果的格式
SINGAL_TEST_FORMAT = "csv"

# 交易参数
PARA_TRADING = {
    "cash": 10000,  # 初始现金、每次交易金额
    "faceValue": 0.001,  # 单张合约面值
    "commission": 4 / 10000,  # 手续费
    "slippage": 1 / 1000,  # 滑点
    "leverage": 3,  # 杠杆
    "marginMin": 1 / 100,  # 最低保障金率，低于爆仓
}

# 交易所参数
EXCHANGE_CONFIG = {
    "options":{
        "defaultType":"future",
    },
    "timeout": 2000,
}