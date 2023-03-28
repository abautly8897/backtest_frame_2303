# 用于回测数据CSV文件的处理
import shutil

import pandas as pd

import trade_cal_handle


# 1. 初始化生成文件夹以及空文件
def initiating(backtest_info):
    asset_df = pd.DataFrame(columns=['date', 'security', 'cash', 'total'])
    holding_df = pd.DataFrame(
        columns=['date', 'code', 'name', 'holding_num', 'in_account_date', 'holding_days'])
    exec_df = pd.DataFrame(columns=['date', 'code', 'price', 'transaction_dir'])
    return asset_df, holding_df, exec_df


# 获取上一个交易日的持仓情况
def get_current_holding_info(date, df):
    pre_date = trade_cal_handle.get_pretrade_date(date)
    target_df = df.loc[df['date'] == pre_date]
    return target_df



def handle_after_backtest(fold_name):
    # 要移动的文件的完整路径
    source_file = 'file.txt'

    # 目标文件夹的完整路径
    target_folder = '/' + fold_name

    # 使用 move() 函数将文件移动到目标文件夹
    shutil.move(source_file, target_folder)
