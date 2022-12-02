import os

## 所有参数设置
symbol = "ETH/USDT"
strategy = "bolling"

# 期望盈利目标（倍数），不满足该倍数的结果不保存
plRate = 3
# 原始数据
# 原始数据的k线级别
levelBase = "5m"
# 原始数据的起止时间
startTimeData = "2020-10-01 00:00:00"
endTimeData = "2022-12-01 00:00:00"
# 原始数据文件的格式和命名
dataFileFmt = "hdf"
dataFile = os.path.join("dataStore", "data_ETH-USDT_1m_20180101_20221110.hdf")
# dataFile = os.path.join("dataStore", f'data_{symbol.replace("/","-")}_{levelBase}_{startTimeData.replace("-","").replace(" ","")[:8]}_{endTimeData.replace("-","").replace(" ","")[:8]}.{dataFileFmt}')

# 测试数据（原始数据中的一部分）
# 测试数据的起止时间
startTimeUse = "2020-10-01 00:00:00"
endTimeUse = "2022-12-01 00:00:00"
# 生成布林带测试参数组合
# levelList = ["5m"]
# maLengthList=[130]
# timesList = [4.6]
levelList = ["5m", "15m", "30m", "1h", "4h", "1d"]
maLengthList=range(10, 2000, 5)
timesList=[i/10 for i in range(5, 50, 1)]

# 生成测试结果的格式
equityFileFmt = "csv"


# 交易参数
paraTrading = {
    "cash": 10000,  # 初始现金、每次交易金额
    "faceValue": 0.001,  # 单张合约面值
    "commission": 4 / 10000,  # 手续费
    "slippage": 1 / 1000,  # 滑点
    "leverage": 3,  # 杠杆
    "marginMin": 1 / 100,  # 最低保障金率，低于爆仓
}