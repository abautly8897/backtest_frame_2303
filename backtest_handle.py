# 用于回测数据CSV文件的处理
import math
import os
import shutil

import pandas as pd

import market_quotation
import security_basic_info
import trade_cal_handle


# 1. 初始化生成文件夹以及空文件
def initiating(backtest_info):
    pre_trade_date = trade_cal_handle.get_pretrade_date(backtest_info[1])
    asset_df = pd.DataFrame(columns=['date', 'security', 'cash', 'total'])
    asset_df = asset_df.append(
        {'date': pre_trade_date, 'security': 0, 'cash': backtest_info[3], 'total': backtest_info[3]}, ignore_index=True)
    holding_df = pd.DataFrame(
        columns=['date', 'code', 'name', 'cost_price', 'last_price', 'return', 'holding_num', 'in_account_date',
                 'holding_days'])
    exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    return asset_df, holding_df, exec_df


# 获取上一个交易日的持仓情况
def prepare_current_asset_holding_info(date, asset_df, holding_df):
    pre_date = trade_cal_handle.get_pretrade_date(date)
    # 每日开始前处理holding_df
    current_holding_df = holding_df.loc[holding_df['date'] == pre_date, :].copy()
    # current_holding_df = holding_df.query('date == ' + pre_date)
    current_holding_df['date'] = [date] * current_holding_df.shape[0]
    holding_sec_ls = current_holding_df.code.tolist()
    for sec in holding_sec_ls:
        date1 = current_holding_df.loc[current_holding_df['code'] == sec, 'in_account_date'].iloc[0]
        current_holding_df.loc[
            current_holding_df['code'] == sec, 'holding_days'] = trade_cal_handle.str_trade_date_delta(date1, date)
    # print(current_holding_df)
    current_holding_df = current_holding_df.sort_values('return', ascending=False)
    current_holding_df.reset_index(drop=True, inplace=True)
    # print(current_holding_df)
    # 每日开始前处理asset_df
    asset_df = asset_df.append(asset_df.iloc[-1], ignore_index=True)
    asset_df.iloc[-1, 0] = date
    # print('current_holding_df', current_holding_df)
    return asset_df, current_holding_df




# 执行卖出操作
def sell_transaction_exec(date, to_sell_dict, asset_df, current_holding_df, exec_price_type='normal'):
    exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, list(to_sell_dict.keys()))
    cash_change = 0
    cash_before_sell = asset_df.iloc[-1, 2]
    sell_sec_list = list(target_sec_price_data.keys())
    for sec in sell_sec_list:
        new_row = {'date': date, 'code': sec, 'price': target_sec_price_data[sec], 'num': to_sell_dict[sec],
                   'transaction_dir': 2}
        exec_df = exec_df.append(new_row, ignore_index=True)
        cash_increase = target_sec_price_data[sec] * to_sell_dict[sec]
        cash_change += cash_increase * 0.999
    cash_after_sell = cash_before_sell + cash_change
    current_holding_df = current_holding_df[~current_holding_df['code'].isin(sell_sec_list)]
    holding_value = 0
    holding_sec = current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)  # 刷新持仓时
    for sec in holding_sec:
        if sec in list(target_sec_price_data.keys()):
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             target_sec_price_data[sec]
        else:
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'].iloc[0]
    new_asset_row = {'date': date, 'security': holding_value, 'cash': cash_after_sell,
                     'total': holding_value + cash_after_sell}
    asset_df.iloc[-1] = new_asset_row
    return exec_df, asset_df, current_holding_df


# 处理买入交易
def buy_transaction_exec(date, to_buy_list, current_holding_df, asset_df, exec_price_type='n'):
    buy_exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    total_asset = asset_df.iloc[-1, 3]
    available_cash = asset_df.iloc[-1, 2]
    single_buy_value = min(available_cash / len(to_buy_list), total_asset / 20)
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, to_buy_list)
    cash_change = 0
    for sec in list(target_sec_price_data.keys()):
        one_hand_num = 200 if sec[:3] == '688' else 100
        buy_num = math.floor(single_buy_value / (target_sec_price_data[sec] * one_hand_num)) * one_hand_num
        new_exec_row = {'date': date, 'code': sec, 'price': target_sec_price_data[sec], 'num': buy_num,
                        'transaction_dir': 1}
        buy_exec_df = buy_exec_df.append(new_exec_row, ignore_index=True)
        cash_change -= target_sec_price_data[sec] * buy_num
        new_holding_row = {'date': date, 'code': sec, 'name': security_basic_info.target_name_transform(sec),
                           'cost_price': target_sec_price_data[sec],
                           'last_price': target_sec_price_data[sec], 'return': 0,
                           'holding_num': buy_num, 'in_account_date': date, 'holding_days': 0}
        current_holding_df = current_holding_df.append(new_holding_row, ignore_index=True)
    cash_after_buy = available_cash + cash_change * 1.001
    holding_value = 0
    holding_sec = current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)
    for sec in holding_sec:
        if sec in list(target_sec_price_data.keys()):
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             target_sec_price_data[sec]
        else:
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'].iloc[0]
    new_asset_row = {'date': date, 'security': holding_value, 'cash': cash_after_buy,
                     'total': holding_value + cash_after_buy}
    asset_df.iloc[-1] = new_asset_row
    return asset_df, current_holding_df, buy_exec_df


# 根据holding信息更新asset_df
def update_asset_df(date, asset_df, current_holding_df):
    holding_value = 0
    available_cash = asset_df.iloc[-1, 2]
    holding_sec = current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)
    for sec in holding_sec:
        if sec in list(target_sec_price_data.keys()):
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             target_sec_price_data[sec]
            current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'] = target_sec_price_data[sec]
            current_holding_df.loc[current_holding_df['code'] == sec, 'return'] = round(
                current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'] / current_holding_df.loc[
                    current_holding_df['code'] == sec, 'cost_price'] - 1, 4)
        else:
            holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                             current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'].iloc[0]
            current_holding_df.loc[current_holding_df['code'] == sec, 'return'] = round(
                current_holding_df.loc[current_holding_df['code'] == sec, 'last_price'] / current_holding_df.loc[
                    current_holding_df['code'] == sec, 'cost_price'] - 1, 4)
    new_asset_row = {'date': date, 'security': holding_value, 'cash': available_cash,
                     'total': holding_value + available_cash}
    current_holding_df = current_holding_df.sort_values('return', ascending=False)
    current_holding_df.reset_index(drop=True, inplace=True)
    asset_df.iloc[-1] = new_asset_row
    return asset_df, current_holding_df


def handle_after_backtest(fold_name):
    # 要移动的文件的完整路径
    source_file = 'file.txt'

    # 目标文件夹的完整路径
    target_folder = '/' + fold_name

    # 使用 move() 函数将文件移动到目标文件夹
    shutil.move(source_file, target_folder)


# 创建文件夹
def make_dir(path):
    folder = os.path.exists(path)

    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print
        "---  new folder...  ---"
        print
        "---  OK  ---"

    else:
        print
        "---  There is this folder!  ---"
