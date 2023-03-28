# 简易回测架构

# 1.遍历回测区间的交易日，获取交易信号
# 2.执行交易，使用前复权价格记录
# 3.通过交易表、持仓表记录策略运行情况


import backtest_handle
import strategy_handle
import trade_cal_handle

all_trade_cal = trade_cal_handle.total_trade_cal_str

# ======回测基础设定======
strategy_name = '业绩断层'
start_date = '20160101'
end_date = '20221231'
initial_cash = 10000000
backtest_setting = [strategy_name, start_date, end_date]

# ======初始化======
# asset_df储存现金和证券金额,每个交易日写入新的一行
# holding_df储存当前持仓证券,每个交易日写入当天仍持仓证券的信息
# exec_df储存交易执行信息
asset_df, holding_df, exec_df = backtest_handle.initiating(backtest_setting)

# ======生成时间序列======
# 根据起始和结束日期生成回测日历
backtest_cal = [x for x in all_trade_cal if end_date >= x >= start_date]

# ======执行回测======
i = 500
# for date in backtest_cal:
current_date = backtest_cal[i]
current_holding_df = backtest_handle.get_current_holding_info(date=current_date, df=holding_df)

to_sell_dict = strategy_handle.get_sell_list(holding_df=current_holding_df)
to_buy_dict = strategy_handle.get_buy_list(date=current_date)

backtest_handle.transaction_exec(transaction_sec_info=to_sell_dict, transaction_dir='2', exec_price_type='n')
backtest_handle.transaction_exec(transaction_sec_info=to_buy_dict, transaction_dir='1', exec_price_type='n')

backtest_handle.handle_after_trade()

# ======回测处理======
result_folder = backtest_handle.creat_result_folder()
backtest_handle.handle_after_backtest()
