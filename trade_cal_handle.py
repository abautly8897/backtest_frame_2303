import datetime

import akshare as ak
import tushare as ts

ts.set_token('e9628663b45f87ea92e39aaa7127063830ebd5090bc1e8943138b84f')
pro = ts.pro_api()


# 获取A股全部交易日
def get_total_trade_cal():
    stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sz399001")
    return stock_zh_index_daily_df['date'].tolist()


# 生成全部交易日
total_trade_cal = get_total_trade_cal()  # date格式
total_trade_cal_str = [dt.strftime('%Y%m%d') for dt in total_trade_cal]  # akshare的交易日历最后一天为真实的最后一个交易日
tushare_total_trade_cal_df = pro.trade_cal()
tushare_cal_ls = tushare_total_trade_cal_df.loc[tushare_total_trade_cal_df['is_open'] == 1, 'cal_date'].to_list()
tushare_cal_ls = sorted(tushare_cal_ls)  # tushare的交易日历直接加上了本年所有交易日


# tushare_cal_ls

# 获取交易日历数据
def get_trade_cal(end_date, start_date='20180101'):
    trade_cal = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date,
                              fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
    return trade_cal


'''
# 获取指定日期的上一个交易日日期
def get_pretrade_date(target_date):
    pretrade_date = pro.trade_cal(exchange='', start_date=target_date, end_date=target_date,
                                  fields='exchange,cal_date,is_open,pretrade_date',
                                  is_open='0').loc[0, 'pretrade_date']
    return pretrade_date
'''


# 获取今天的日期，str格式，对于交易日收盘以后需做特殊处理
def get_today_date():
    now = datetime.datetime.now()
    today = datetime.date.today()
    if today in total_trade_cal:
        if now.hour >= 15:
            today = today + datetime.timedelta(days=1)
    today = today.strftime("%Y%m%d")
    return today


# 获取指定日期的上一个交易日日期
def get_pretrade_date(origin_date, total_trade_cal_str=tushare_cal_ls):
    try:
        index = total_trade_cal_str.index(origin_date)
        target_date = total_trade_cal_str[index - 1]
    except:
        new_list = [x for x in total_trade_cal_str if x < origin_date]
        target_date = new_list[-1]
    return target_date


# 获取指定日期的下一个交易日日期
def get_next_trade_date(target_date, total_trade_cal_str=tushare_cal_ls):
    new_list = [x for x in total_trade_cal_str if x > target_date]
    # index = total_trade_cal_str.index(target_date)
    return new_list[0]


# 计算两个date差几天
def date_delta(date1, date2):
    delta = date2 - date1
    return delta.days


def str_trade_date_delta(date1, date2, trade_cal_ls=tushare_cal_ls):
    if date2 in trade_cal_ls:  # 若今天不是交易日
        current_index = trade_cal_ls.index(date2)
    else:
        current_index = trade_cal_ls.index(get_pretrade_date(date2))
    previous_index = trade_cal_ls.index(date1)
    return current_index - previous_index


def is_today_trading_day():
    now = datetime.datetime.now()
    today = datetime.date.today()
    result = False
    if today.strftime("%Y%m%d") in tushare_cal_ls:
        if now.hour >= 15:
            result = False
        else:
            result = True
    return result


def nature_date_offset(date_str, n):  # 给定的日期字符串，例如 '20220322'
    # 将字符串转换为 datetime 对象
    date = datetime.datetime.strptime(date_str, '%Y%m%d')
    # 生成前 n 天日期
    date = date - datetime.timedelta(days=n)
    # 将日期对象转换为字符串，格式为 'yyyymmdd'
    return date.strftime('%Y%m%d')


# 分钟增减
def subtract_minutes(time_str, minutes):
    # 解析时间字符串为 time 对象
    time_obj = datetime.datetime.strptime(time_str, "%H:%M:%S").time()
    # 将 time 对象转换为 datetime 对象
    datetime_obj = datetime.datetime.combine(datetime.datetime.today(), time_obj)
    # 减去指定分钟数
    new_datetime_obj = datetime_obj - datetime.timedelta(minutes=minutes)
    # 返回新的时间字符串
    return new_datetime_obj.strftime("%H:%M:%S")


# 秒钟增减
def subtract_seconds(time_str, seconds):
    # 解析时间字符串为 time 对象
    time_obj = datetime.datetime.strptime(time_str, "%H:%M:%S").time()
    # 将 time 对象转换为 datetime 对象
    datetime_obj = datetime.datetime.combine(datetime.datetime.today(), time_obj)
    # 减去指定分钟数
    new_datetime_obj = datetime_obj - datetime.timedelta(seconds=seconds)
    # 返回新的时间字符串
    return new_datetime_obj.strftime("%H:%M:%S")


# 计算两个HHMM格式的时间之间相差多少分钟
def time_diff_in_minutes(time1, time2):
    # 将两个时间字符串转换为datetime对象
    dt1 = datetime.datetime.strptime(time1, '%H%M')
    dt2 = datetime.datetime.strptime(time2, '%H%M')

    # 计算时间差
    diff = abs(dt2 - dt1)

    # 将时间差转换为分钟数并返回
    diff_in_minutes = diff.seconds // 60
    return diff_in_minutes


# 计算当天已经交易了多少分钟
def count_traded_minutes(current_time):
    if int(current_time) < 930:
        traded_minutes = 0
    elif 930 <= int(current_time) <= 1130:
        traded_minutes = max(1, time_diff_in_minutes('0930', current_time))
    elif int(current_time) <= 1300:
        traded_minutes = 120
    elif int(current_time) <= 1500:
        traded_minutes = time_diff_in_minutes('1300', current_time) + 120
    else:
        traded_minutes = 240
    return traded_minutes


# 交易日位移
def trade_date_offset(date, n, cal_ls=tushare_cal_ls):
    if date not in cal_ls:
        date = get_pretrade_date(origin_date=date)
    current_index = cal_ls.index(date)
    new_index = current_index + n
    return cal_ls[new_index]


# 获取前推n年的报告期列表
def get_period_list(last_period, n):
    period_list = ['1'] * n
    for i in range(n):
        period_list[n - i - 1] = str(int(last_period[:4]) - i) + last_period[4:]
    return period_list

# 生成业绩报告期
'''def get_yj_period(date):
    year = date[:4]
    short_date = date[4:]
    if short_date <= '0430':
        yj_peridos = [year + '0331', str(int(year) - 1) + '1231',year + '0630']
    elif short_date <= '0830':
        yj_peridos = [year + '0630',year + '0930',year + '1231']
    elif short_date <= '1030':
        yj_peridos = [year + '0930',year + '1231']
    elif short_date <= '1101':
        yj_peridos = [year + '1231']
    return yj_peridos'''
def get_yj_period(date):
    year = date[:4]
    short_date = date[4:]
    if short_date <= '0430':
        yj_peridos = [str(int(year) - 1) + '1231']
    elif short_date <= '0830':
        yj_peridos = [year + '0630']
    elif short_date <= '1030':
        yj_peridos = [year + '0930']
    elif short_date <= '1101':
        yj_peridos = [year + '1231']
    return yj_peridos


# 获取两个日期之间的日期list
def get_days_between(start_date, end_date, end_included='n'):
    date_list = []
    start = datetime.datetime.strptime(start_date, "%Y%m%d")
    end = datetime.datetime.strptime(end_date, "%Y%m%d")
    delta = datetime.timedelta(days=1)
    if end_included != 'n':
        while start <= end:
            date_list.append(start.strftime("%Y%m%d"))
            start += delta
    else:
        start += delta
        while start < end:
            date_list.append(start.strftime("%Y%m%d"))
            start += delta
    return date_list
