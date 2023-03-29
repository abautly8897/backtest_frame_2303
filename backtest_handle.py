# 用于回测数据CSV文件的处理
import math
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
        columns=['date', 'code', 'name', 'holding_num', 'in_account_date', 'holding_days'])
    exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    return asset_df, holding_df, exec_df


# 获取上一个交易日的持仓情况
def prepare_current_asset_holding_info(date, asset_df, holding_df):
    pre_date = trade_cal_handle.get_pretrade_date(date)
    # current_holding_df = holding_df.loc[holding_df['date'] == pre_date, :]
    current_holding_df = holding_df.query('date == ' + pre_date)
    current_holding_df.loc[:, 'date'] = [date] * current_holding_df.shape[0]
    asset_df = asset_df.append(asset_df.iloc[-1], ignore_index=True)
    asset_df.iloc[-1, 0] = date
    return asset_df, current_holding_df




# 执行卖出操作
def sell_transaction_exec(transaction_sec_info, date, exec_price_type='normal'):
    exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, list(transaction_sec_info.keys()))
    cash_change = 0
    for key in transaction_sec_info:
        new_row = {'date': date, 'code': key, 'price': target_sec_price_data[key], 'num': transaction_sec_info[key],
                   'transaction_dir': 2}
        exec_df = exec_df.append(new_row, ignore_index=True)
        cash_increase = target_sec_price_data[key] * transaction_sec_info[key]
        cash_change += cash_increase
    return exec_df, cash_change


# 卖出后记录相关信息
def asset_holding_info_handle(date, asset_df, current_holding_df, sell_exec_df, cash_change):
    sell_sec_list = sell_exec_df.code.tolist()
    cash_before_sell = asset_df.iloc[-1, 2]
    cash_after_sell = cash_before_sell + cash_change
    new_current_holding_df = current_holding_df[~current_holding_df['code'].isin(sell_sec_list)]
    holding_value = 0
    holding_sec = new_current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)
    for sec in holding_sec:
        holding_value += new_current_holding_df[new_current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                         target_sec_price_data[sec]
    new_asset_row = {'date': date, 'security': holding_value, 'cash': cash_after_sell,
                     'total': holding_value + cash_after_sell}
    asset_df.iloc[-1] = new_asset_row
    return asset_df


# 处理买入交易
def buy_transaction_exec(date, to_buy_list, current_holding_df, asset_df, exec_price_type='n'):
    buy_exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'num', 'transaction_dir'])
    available_cash = asset_df.iloc[-1, 2]
    single_buy_value = min(available_cash / len(to_buy_list), available_cash / 15)
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, to_buy_list)
    cash_change = 0
    for sec in to_buy_list:
        one_hand_num = 200 if sec[:3] == '688' else 100
        buy_num = math.floor(single_buy_value / (target_sec_price_data[sec] * one_hand_num)) * one_hand_num
        new_exec_row = {'date': date, 'code': sec, 'price': target_sec_price_data[sec], 'num': buy_num,
                        'transaction_dir': 1}
        buy_exec_df = buy_exec_df.append(new_exec_row, ignore_index=True)
        cash_change -= target_sec_price_data[sec] * buy_num
        new_holding_row = {'date': date, 'code': sec, 'name': security_basic_info.target_name_transform(sec),
                           'holding_num': buy_num, 'in_account_date': date, 'holding_days': 0}
        current_holding_df = current_holding_df.append(new_holding_row, ignore_index=True)
    cash_after_buy = available_cash + cash_change
    holding_value = 0
    holding_sec = current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)
    for sec in holding_sec:
        holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                         target_sec_price_data[sec]
    new_asset_row = {'date': date, 'security': holding_value, 'cash': cash_after_buy,
                     'total': holding_value + cash_after_buy}
    asset_df.iloc[-1] = new_asset_row
    return asset_df, current_holding_df, buy_exec_df


# 根据holding信息更新asset_df
def renew_asset_df(date, asset_df, current_holding_df):
    holding_value = 0
    available_cash = asset_df.iloc[-1, 2]
    holding_sec = current_holding_df.code.tolist()
    target_sec_price_data = market_quotation.get_target_sec_price_data(date, holding_sec)
    for sec in holding_sec:
        holding_value += current_holding_df.loc[current_holding_df['code'] == sec, 'holding_num'].iloc[0] * \
                         target_sec_price_data[sec]
    new_asset_row = {'date': date, 'security': holding_value, 'cash': available_cash,
                     'total': holding_value + available_cash}
    asset_df.iloc[-1] = new_asset_row
    return asset_df


def handle_after_backtest(fold_name):
    # 要移动的文件的完整路径
    source_file = 'file.txt'

    # 目标文件夹的完整路径
    target_folder = '/' + fold_name

    # 使用 move() 函数将文件移动到目标文件夹
    shutil.move(source_file, target_folder)
