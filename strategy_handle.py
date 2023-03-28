import datetime

import akshare as ak
import pandas as pd

import security_basic_info
import trade_cal_handle


# 策略逻辑


# 获取当天应卖出的证券
def get_sell_list(holding_df):
    target_sec_df = holding_df.loc[holding_df['holding_days'] > 40]
    security_list = target_sec_df.code.tolist()
    return security_list


# 获取当天应买入的证券
def get_buy_list(date):
    get_report_release_sec(date)
    return []


# 根据日期获取需观测的证券
def get_report_release_sec(date_string=trade_cal_handle.get_today_date()):
    pretrade_date = trade_cal_handle.get_pretrade_date(date_string)
    prepre_trade_date = trade_cal_handle.get_pretrade_date(pretrade_date)
    today_date = datetime.datetime.strptime(date_string, '%Y%m%d').date()
    pretrade_date = datetime.datetime.strptime(pretrade_date, '%Y%m%d').date()
    prepre_trade_date = datetime.datetime.strptime(prepre_trade_date, '%Y%m%d').date()
    # 查看前两个交易日是否相邻，若相邻则仅需考察T-1交易日发布业绩的证券，若不相邻还需加入两个交易日之间发布业绩的证券
    pretrade_date_delta = trade_cal_handle.date_delta(prepre_trade_date, pretrade_date)
    # 获取业绩数据
    current_yj_period = trade_cal_handle.get_yj_period(date_string)  # 设定报告期
    print(current_yj_period)
    stock_yjyg_em_df = ak.stock_yjyg_em(date=current_yj_period[0])  # 业绩预告数据
    stock_yjkb_em_df = ak.stock_yjkb_em(date=current_yj_period[0])  # 业绩快报数据
    stock_yjbb_em_df = ak.stock_yjbb_em(date=current_yj_period[0])  # 正式报表数据
    # 根据情况筛选数据
    if pretrade_date_delta == 1:
        secCode_yjyg = stock_yjyg_em_df.loc[
            stock_yjyg_em_df['公告日期'] == pretrade_date, '股票代码'].tolist()  # 指定日期发布业绩预告的证券
        secCode_yjkb = stock_yjkb_em_df.loc[
            stock_yjkb_em_df['公告日期'] == pretrade_date, '股票代码'].tolist()  # 指定日期发布业绩快报的证券
        secCode_yjbb = stock_yjbb_em_df.loc[
            stock_yjbb_em_df['最新公告日期'] == pretrade_date, '股票代码'].tolist()  # 指定日期发布业绩公告的证券
    else:
        secCode_yjyg = stock_yjyg_em_df.loc[(stock_yjyg_em_df['公告日期'] > prepre_trade_date) & (
                stock_yjyg_em_df['公告日期'] < today_date), '股票代码'].tolist()  # 指定日期发布业绩预告的证券
        secCode_yjkb = stock_yjkb_em_df.loc[(stock_yjkb_em_df['公告日期'] > prepre_trade_date) & (
                stock_yjkb_em_df['公告日期'] < today_date), '股票代码'].tolist()  # 指定日期发布业绩快报的证券
        secCode_yjbb = stock_yjbb_em_df.loc[(stock_yjbb_em_df['最新公告日期'] > prepre_trade_date) & (
                stock_yjbb_em_df['最新公告日期'] < today_date), '股票代码'].tolist()  # 指定日期发布业绩公告的证券
    secCode_fbyj = list(set(secCode_yjyg + secCode_yjkb + secCode_yjbb))
    secCode_fbyj = [x for x in secCode_fbyj if x in security_basic_info.total_sec_list]
    secCode_fbyj = sorted(secCode_fbyj)
    secCode_fbyj_name_list = [security_basic_info.target_name_transform(x) for x in secCode_fbyj]
    return pd.DataFrame({'Code': secCode_fbyj, 'Name': secCode_fbyj_name_list})
