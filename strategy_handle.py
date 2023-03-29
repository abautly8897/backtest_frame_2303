import datetime

import tushare as ts

ts.set_token('e9628663b45f87ea92e39aaa7127063830ebd5090bc1e8943138b84f')
pro = ts.pro_api()

import pandas as pd

import market_quotation
import security_basic_info
import trade_cal_handle


# 策略逻辑


# 获取当天应卖出的证券
def get_sell_list(holding_df):
    target_sec_df = holding_df.loc[holding_df['holding_days'] > 40]
    security_list = target_sec_df.code.tolist()
    to_sell_num_list = target_sec_df.holding_num.tolist()
    return dict(zip(security_list, to_sell_num_list))


# 获取当天应买入的证券
def get_buy_list(date):
    picked_report_release_sec_ls = pick_report_release_sec(target_date=date)  # 找出发布业绩的证券
    picked_report_release_sec_ls = [x for x in picked_report_release_sec_ls if
                                    x in security_basic_info.total_sec_list]  # 剔除总表外的证券
    target_sec_df = pd.DataFrame({'Code': picked_report_release_sec_ls,
                                  'Name': [security_basic_info.target_name_transform(x) for x in
                                           picked_report_release_sec_ls]})
    stocks = target_sec_df.Code.tolist()  # 上证股票代码以sh开头，深证股票代码以sz开头
    stocks = security_basic_info.kick_new_share(sec_list=stocks, n=60, date=date)  # 排除新股、次新股
    target_sec_hist = market_quotation.target_stock_daily_hist(stocks=stocks, n=6, date=date)  # 获取T-1日行情
    picked_security = []
    if target_sec_hist.shape[0] > 0:
        Tn1_ana_result = get_Tn1_ana_result(target_sec_hist)  # 根据T-1日的行情筛选
        picked_security = Tn1_ana_result.loc[Tn1_ana_result['Simple_Result'] == True, 'Code'].tolist()  # 输出为list
    print('T-1日行情符合要求:{}'.format(picked_security))
    to_buy_security = []
    if picked_security != []:
        to_buy_security = Tday_check(list=picked_security, date=date)
    return to_buy_security


# 获取单日的发布报告证券
def get_report_release_sec(target_date=trade_cal_handle.get_today_date()):
    select_ls = pro.balancesheet_vip(ann_date=target_date,
                                     fields='ts_code,ann_date,f_ann_date,end_date').ts_code.tolist()
    yjbg_sec_ls = [x[0:6] for x in select_ls if x[0] in ['0', '3', '6']]
    select_ls = pro.forecast(ann_date=target_date, fields='ts_code,ann_date,end_date').ts_code.tolist()
    yjyg_sec_ls = [x[0:6] for x in select_ls if x[0] in ['0', '3', '6']]
    select_ls = pro.express(ann_date=target_date, fields='ts_code,ann_date,end_date').ts_code.tolist()
    yjkb_sec_ls = [x[0:6] for x in select_ls if x[0] in ['0', '3', '6']]
    release_report_sec_ls = list(set(yjbg_sec_ls) | set(yjyg_sec_ls) | set(yjkb_sec_ls))
    return release_report_sec_ls


# 根据日期获取需观测的证券
def pick_report_release_sec(target_date=datetime.date.today().strftime("%Y%m%d")):
    pre_trade_date = trade_cal_handle.get_pretrade_date(target_date)
    prepre_trade_date = trade_cal_handle.get_pretrade_date(pre_trade_date)
    pre_date_datetime = datetime.datetime.strptime(pre_trade_date, '%Y%m%d').date()
    prepre_date_datetime = datetime.datetime.strptime(prepre_trade_date, '%Y%m%d').date()
    # 查看前两个交易日是否相邻，若相邻则仅需考察T-1交易日发布业绩的证券，若不相邻还需加入两个交易日之间发布业绩的证券
    pretrade_date_delta = trade_cal_handle.date_delta(prepre_date_datetime, pre_date_datetime)
    picked_report_release_sec_ls = get_report_release_sec(target_date=pre_trade_date)
    if pretrade_date_delta > 1:
        extra_dates = trade_cal_handle.get_days_between(prepre_trade_date, pre_trade_date, end_included='n')
        for date in extra_dates:
            picked_report_release_sec_ls.extend(get_report_release_sec(target_date=date))
    picked_report_release_sec_ls = list(set(picked_report_release_sec_ls))
    return picked_report_release_sec_ls


# 生成T-1行情的分析结果
def get_Tn1_ana_result(target_sec_hist):
    security_ls = list(set(target_sec_hist.secCode.tolist()))
    target_sec_result_df = pd.DataFrame({'Code': security_ls,
                                         'Name': [security_basic_info.target_name_transform(x) for x in
                                                  security_ls]})
    result_df = pd.DataFrame()
    simple_result_ls = []
    num_true_ls = []
    for sec in security_ls:
        price_data = target_sec_hist[target_sec_hist['secCode'] == sec]
        price_data = price_data.reset_index(drop=True)
        # print(sec)
        single_result_ls = single_condition_check(price_data)
        simple_result_ls.append(all(single_result_ls))
        num_true = sum([1 if x else 0 for x in single_result_ls])
        num_true_ls.append(num_true)
        result_df = result_df.append(pd.DataFrame(single_result_ls).T, ignore_index=True)

        # result_list.append(single_result)
    n = 5
    condition_list = ["condition_{}".format(x) for x in range(1, n + 1)]
    result_df.columns = condition_list
    target_sec_result_df['Simple_Result'] = simple_result_ls
    target_sec_result_df['True_num'] = num_true_ls
    target_sec_result_df = pd.concat([target_sec_result_df, result_df], axis=1)
    return target_sec_result_df


# 单票查看
def single_condition_check(price_data):
    close_list = list(price_data['收盘'])
    high_list = list(price_data['最高'])
    low_list = list(price_data['最低'])
    open_list = list(price_data['开盘'])
    last_close_return = close_list[-1] / close_list[-2] - 1
    last_open_rate = open_list[-1] / close_list[-2] - 1
    last_inday_change = close_list[-1] / open_list[-1] - 1
    last_gap = low_list[-1] / high_list[-2]
    nday_return = close_list[-2] / open_list[0] - 1
    # 设置条件
    condition_1 = last_open_rate > 0.03  # 业绩发布后，第二天（T-1）至少高开3%
    # condition_2 = high_limit>last_close_return> g.T1_close #  T-1日收高
    condition_2_1 = (0.16 > last_close_return > 0.1 and last_open_rate > 0.08)  # T-1日收高
    condition_2_2 = 0.099 > last_close_return > 0.03  # T-1日收高
    # condition_3 = (close_list[-1]/close_list[0]-1)<0.12 # 近x个交易日涨幅小于阈值
    condition_4 = (close_list[-1] / close_list[0] - 1) > -0.02  # 近x个交易日跌小于阈值
    condition_5 = last_gap > 0.99  # 形成一定程度缺口
    condition_6_1 = -0.02 < last_inday_change < 0.06 and last_open_rate < 0.05  # T-1日内涨跌幅在一定区间之内
    condition_6_2 = -0.03 < last_inday_change < 0.08 and last_open_rate >= 0.05  # T-1日内涨跌幅在一定区间之内
    # condition_3 = low_list[-1]*0.99 > mean(low_list[0:-1])  # 盘中不创新低
    # condition_6 = max(high_list)<high_list[-1] # 盘中创新高
    '''
    if condition_1 and (condition_2_1 or condition_2_2) and condition_4 and condition_5 and (
            condition_6_1 or condition_6_2):
        result = True
    else:
        result = False
    '''
    result = [condition_1, (condition_2_1 or condition_2_2), condition_4, condition_5, (condition_6_1 or condition_6_2)]
    return result


# 判断是否ST，通过获取实时行情查看是都停牌、涨停以及符合行情要求
def Tday_check(list, date):
    new_ls = market_quotation.ST_check(list)  # 排除ST标的
    new_ls = market_quotation.pause_check(new_ls, date)  # 排除停牌标的
    df = pro.daily(start_date=date, end_date=date)
    df['ts_code'] = [x[:6] for x in df.ts_code.tolist()]
    result_ls = []
    for sec in new_ls:
        tick = dict(zip(df.columns.tolist(), df.loc[df['ts_code'] == sec].iloc[0]))  # 获取实时价格
        if tick['open'] < tick['pre_close'] * 1.098:  # 检查是否涨停
            if -0.015 < tick['open'] / tick['pre_close'] - 1 < 0.08:  # 排除低开超过阈值的证券,或高开过多的证券
                result_ls.append(sec)
            else:
                print('证券【{}】因低开或高开过多排除。'.format(security_basic_info.target_name_transform(sec) + sec))
        else:
            print('证券【{}】因涨停排除。'.format(security_basic_info.target_name_transform(sec) + sec))
        # buy_plan_message = '根据备选证券盘初行情判定买入证券：{}'.format([security_basic_info.target_name_transform(x) + x for x in result_ls])
        # send_email.send_msg_with_mail('今日买入计划', buy_plan_message)
        # print(buy_plan_message)
        return result_ls
