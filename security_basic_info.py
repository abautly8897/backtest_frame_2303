import akshare as ak
import tushare as ts

import trade_cal_handle

ts.set_token('e9628663b45f87ea92e39aaa7127063830ebd5090bc1e8943138b84f')
pro = ts.pro_api()


# 从akshare下载数据
# code_name_df = ak.stock_info_a_code_name()
# total_sec_list = code_name_df['code'].tolist()

# 从tushare系在数据
# code_name_df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
# code_name_df.columns = ['tu_code', 'code', 'name', 'area', 'industry', 'list_date']
# total_sec_list = code_name_df['code'].tolist()

def get_code_and_name_df():
    try:
        code_name_df = pro.stock_basic(exchange='', list_status='L',
                                       fields='ts_code,symbol,name,area,industry,list_date')
        code_name_df.columns = ['tu_code', 'code', 'name', 'area', 'industry', 'list_date']
        total_sec_list = code_name_df['code'].tolist()

    except Exception:
        code_name_df = ak.stock_info_a_code_name()
        total_sec_list = code_name_df['code'].tolist()
    return code_name_df, total_sec_list


code_name_df, total_sec_list = get_code_and_name_df()


# 标的代码转换名称
def target_name_transform(secCode, code_name_df=code_name_df):
    secCode = secCode[0:6]
    result = code_name_df.loc[code_name_df['code'] == secCode, 'name'].iloc[0]
    return result


# 将简单的symbol类型的证券代码转换成tushare代码
def trans_symbol_to_tscode(sec):
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    ts_code = data.loc[data['symbol'] == sec, 'ts_code'].values[0]
    return ts_code


# 将简单的symbol类型的证券代码转换成腾讯实时行情所需的代码
def sym_to_tencent_code(s):
    if s[0] == '6':
        s = 'sh' + s
    elif s[0] in ('0', '3'):
        s = 'sz' + s
    else:
        print('无法识别的代码')
    return s


# 根据证券代码确定交易所
def exchange_check(s):
    if len(s) == 6:
        if s[0] == '6':
            ex_type = '1'
        elif s[0] in ('0', '3'):
            ex_type = '2'
    else:
        ex_type = 'S'
    return ex_type


# 获取证券的上市日期
def get_list_date(sec):
    try:
        list_date = code_name_df.loc[code_name_df['code'] == sec, 'list_date'].iloc[0]
    except Exception:
        list_date = ak.stock_individual_info_em(symbol=sec).iloc[3, 1]
    return list_date


# 剔除新股
def kick_new_share(sec_list, n, date=trade_cal_handle.get_today_date()):
    target_date = trade_cal_handle.trade_date_offset(date, n * (-1))
    result_ls = []
    for sec in sec_list:
        list_date = get_list_date(sec=sec)
        result_ls.append(False if list_date > target_date else True)
    result_sec_list = [sec_list[i] for i in range(len(sec_list)) if result_ls[i] == True]
    return result_sec_list


# 获取某个日期已上市证券的list
def get_all_security(date, result_type):
    '''
    根据日期参数，返回当时仍在上市的证券的代码
    :param date: 'yyyymmdd'
    :param result_type:'symbol'、'ts_code'
    :return:list
    '''
    data = pro.query('stock_basic', exchange='', list_status='L',
                     fields='ts_code,symbol,name,area,industry,list_date,exchange')
    data = data.loc[(data['exchange'] == 'SZSE') | (data['exchange'] == 'SSH'),]
    return data.loc[data['list_date'] <= date, result_type].tolist()
