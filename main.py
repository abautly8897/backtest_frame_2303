# 简易回测架构
import pandas as pd

import backtest_handle
import security_basic_info
import strategy_handle
import trade_cal_handle

# 1.遍历回测区间的交易日，获取交易信号
# 2.执行交易，使用前复权价格记录
# 3.通过交易表、持仓表记录策略运行情况

all_trade_cal = trade_cal_handle.total_trade_cal_str

# ======回测基础设定======
strategy_name = '业绩断层'
start_date = '20230301'
end_date = '20230328'
initial_cash = 10000000
backtest_setting = [strategy_name, start_date, end_date, initial_cash]

# ======初始化======
# asset_df储存现金和证券金额,每个交易日写入新的一行
# holding_df储存当前持仓证券,每个交易日写入当天仍持仓证券的信息
# exec_df储存交易执行信息
asset_df, holding_df, exec_df = backtest_handle.initiating(backtest_setting)

# ======生成时间序列======
# 根据起始和结束日期生成回测日历
backtest_cal = [x for x in all_trade_cal if end_date >= x >= start_date]

# ======执行回测======
i = 50
for i in range(len(backtest_cal)):
    current_date = backtest_cal[i]
    print('【{}】'.format(current_date))
    asset_df, current_holding_df = backtest_handle.prepare_current_asset_holding_info(date=current_date,
                                                                                      asset_df=asset_df,
                                                                                      holding_df=holding_df)
    to_sell_dict = strategy_handle.get_sell_list(holding_df=current_holding_df)
    to_sell_list = list(to_sell_dict.keys())
    to_buy_list = strategy_handle.get_buy_list(date=current_date)

    # ======选股信息打印======

    picked_security_info = '今日盘前筛选出{}只大致符合条件的股票：{}'.format(
        len(to_buy_list), [security_basic_info.target_name_transform(x) + x for x in to_buy_list])
    print(picked_security_info)
    to_sell_security_info = '今日计划持有到期或止损 {} 只股票：{}'.format(
        len(to_sell_list), [security_basic_info.target_name_transform(x) + x for x in to_sell_list])
    print(to_sell_security_info)

    # ======交易处理======
    if len(to_sell_list) > 0:
        sell_exec_df, cash_change = backtest_handle.sell_transaction_exec(transaction_sec_info=to_sell_dict,
                                                                          date=current_date,
                                                                          exec_price_type='normal')
        exec_df = pd.concat([exec_df, sell_exec_df], ignore_index=True)
        asset_df, current_holding_df = backtest_handle.asset_holding_info_handle(date=current_date, asset_df=asset_df,
                                                                                 current_holding_df=current_holding_df,
                                                                                 sell_exec_df=sell_exec_df,
                                                                                 cash_change=cash_change)

    if len(to_buy_list) > 0:
        asset_df, current_holding_df, buy_exec_df = backtest_handle.buy_transaction_exec(date=current_date,
                                                                                         to_buy_list=to_buy_list,
                                                                                         current_holding_df=current_holding_df,
                                                                                         asset_df=asset_df,
                                                                                         exec_price_type='n')
        exec_df = pd.concat([exec_df, buy_exec_df], ignore_index=True)

    # backtest_handle.handle_after_trade()

    # ======回测处理======
    # result_folder = backtest_handle.creat_result_folder()
    # backtest_handle.handle_after_backtest()
