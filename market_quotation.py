import datetime
import akshare as ak
import security_basic_info
import requests
import trade_cal_handle
import pandas as pd


# 检查是否ST
def ST_check(sec_ls):
    stock_zh_a_st_em_df = ak.stock_zh_a_st_em()
    current_st_ls = stock_zh_a_st_em_df['代码'].tolist()
    not_ST_ls = []
    for sec in sec_ls:
        if sec not in current_st_ls:
            not_ST_ls.append(sec)
        else:
            print(security_basic_info.target_name_transform(sec), sec, '今日停牌')
    return not_ST_ls


# 检查是否停牌，通过AKShare 相关接口判断是否停牌
# ak.stock_tfp_em()
def pause_check(sec_ls):
    stock_tfp_em_df = ak.stock_tfp_em()
    current_pause_ls = stock_tfp_em_df['代码'].tolist()
    not_pause_ls = []
    for sec in sec_ls:
        if sec not in current_pause_ls:
            not_pause_ls.append(sec)
    return not_pause_ls


# 通过腾讯api获取个股实时行情
class Tick:
    """ 实时行情数据"""
    symbol: str  # 股票代码
    seccode: str  # 股票代码2
    name: str  # 股票名称
    percent: float  # 涨跌幅度
    updown: float  # 涨跌点数
    open: float  # 今日开盘价
    yesterday_close: float  # 昨日收盘价
    last: float  # 当前价格
    high: float  # 今日最高价
    low: float  # 今日最低价
    high_limit: float  # 今日涨停价
    low_limit: float  # 今日跌停价
    bid_price: float  # 竞买价
    ask_price: float  # 竞卖价
    transactions: int  # 成交数量
    turnover: float  # 成交金额
    bid1_quantity: int  # 买一数量
    bid1_price: float  # 买一报价
    bid2_quantity: int  # 买二数量
    bid2_price: float  # 买二报价
    bid3_quantity: int  # 买三数量
    bid3_price: float  # 买三报价
    bid4_quantity: int  # 买四数量
    bid4_price: float  # 买四报价
    bid5_quantity: int  # 买五数量
    bid5_price: float  # 买五报价
    ask1_quantity: int  # 卖一数量
    ask1_price: float  # 卖一报价
    ask2_quantity: int  # 卖二数量
    ask2_price: float  # 卖二报价
    ask3_quantity: int  # 卖三数量
    ask3_price: float  # 卖三报价
    ask4_quantity: int  # 卖四数量
    ask4_price: float  # 卖四报价
    ask5_quantity: int  # 卖五数量
    ask5_price: float  # 卖五报价
    timestamp: str  # 时间戳


# 通过腾讯api获取个股实时行情
def get_tick_price(code):
    code = security_basic_info.sym_to_tencent_code(code)
    response = requests.get(f"http://qt.gtimg.cn/q={code}").text
    data = str(response).split('~')
    tick = Tick()
    tick.last = float(data[3])  # 最新价
    tick.name = data[1]  # 证券名称
    tick.symbol = data[2]  # 000000格式的证券代码
    tick.seccode = data[0][2:10]  # xx000000格式的证券代码
    tick.percent = float(data[32]) / 100  # 涨跌幅
    tick.updown = float(data[31])  # 涨跌额
    tick.high = float(data[33])  # 最高价
    tick.yesterday_close = float(data[4])  # 昨收价
    tick.open = float(data[5])  # 开盘价
    tick.transactions = int(data[6])  # 成交数量
    tick.turnover = float(data[37])  # 成交金额
    tick.high = float(data[33])  # 今日最高价
    tick.low = float(data[34])  # 今日最低价
    tick.high_limit = float(data[47])  # 今日涨停价
    tick.low_limit = float(data[48])  # 今日跌停价
    tick.bid1_quantity = int(data[10])  # 买一数量
    tick.bid1_price = float(data[9])  # 买一报价
    tick.bid2_quantity = int(data[12])  # 买二数量
    tick.bid2_price = float(data[11])  # 买二报价
    tick.bid3_quantity = int(data[14])  # 买三数量
    tick.bid3_price = float(data[13])  # 买三报价
    tick.bid4_quantity = int(data[16])  # 买四数量
    tick.bid4_price = float(data[15])  # 买四报价
    tick.bid5_quantity = int(data[18])  # 买五数量
    tick.bid5_price = float(data[17])  # 买五报价
    tick.ask1_quantity = int(data[20])  # 卖一数量
    tick.ask1_price = float(data[19])  # 卖一报价
    tick.ask2_quantity = int(data[22])  # 卖二数量
    tick.ask2_price = float(data[21])  # 卖二报价
    tick.ask3_quantity = int(data[24])  # 卖三数量
    tick.ask3_price = float(data[23])  # 卖三报价
    tick.ask4_quantity = int(data[26])  # 卖四数量
    tick.ask4_price = float(data[25])  # 卖四报价
    tick.ask5_quantity = int(data[28])  # 卖五数量
    tick.ask5_price = float(data[27])  # 卖五报价
    tick.timestamp = str(data[30])  # 时间戳
    return tick


# 获取实时行情
def real_time_quotation_check(tick):
    today_string = datetime.date.today().strftime("%Y%m%d")
    if today_string == tick.timestamp[0:8]:
        print('以获取实时行情。')
        result = True
    else:
        print('未能获取实时行情或当日停牌或未开市。')
        result = False
    return result


# 获取指定日期一组证券（list）前推n天的行情信息
def target_stock_daily_hist(stocks, n=2,
                            date=datetime.date.today().strftime("%Y%m%d")):  # date默认今天的日期，行情天数默认为2天，即为T-1和T-2日的行情

    # 指定日期
    today_string = date
    end_date = trade_cal_handle.get_pretrade_date(today_string)  # 日期格式为YYYYMMDD
    total_trade_cal = trade_cal_handle.get_total_trade_cal()
    total_trade_cal_str = [dt.strftime('%Y%m%d') for dt in total_trade_cal]
    end_date_index = total_trade_cal_str.index(end_date)
    start_date = total_trade_cal_str[0:end_date_index][-(n - 1)]

    # 获取多只证券在指定日期的行情数据
    stock_data = pd.DataFrame()
    for stock in stocks:
        data = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date=start_date, end_date=end_date, adjust="")
        data['secCode'] = [stock] * data.shape[0]
        data['name'] = [security_basic_info.target_name_transform(stock)] * data.shape[0]
        stock_data = stock_data.append(data)
        stock_data = stock_data.reset_index(drop=True)
    return stock_data
