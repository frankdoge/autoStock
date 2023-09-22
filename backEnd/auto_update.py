import redis
import easyquotation
import json
import pandas as pd
import psycopg2
import tushare as ts
import datetime
from io import StringIO
from tools import *
conn = psycopg2.connect(database='stock', user='postgres',password='csubigdata',host='localhost',port='5432')

cur = conn.cursor()

pro = ts.pro_api('ebd374898a15c69a938b0b6c2de6beb2854b694a4bbe6c731640bfc4')


# 通过一次查询把股票代码和股票名称对应起来
dat = pro.query('stock_basic', fields='symbol,name')
def get_name(stock_code):
    # 通过股票代码获得名称
    company_name = list(dat.loc[dat['symbol'] == stock_code].name)
    # 没有查到这个股票代码
    if len(company_name) == 0:
        return 'null'
    company_name = company_name[0]
    return company_name


# 将重排索引处理好的数据转换为数据库格式
def todatabase(daily):
    new_df = pd.DataFrame(columns=('date','code','name',"open", "close", "high", "low", "volume", "money", "avg","high_limit","low_limit","pre_close","paused"))
    new_df['date'] = daily['trade_date']
    new_df['code'] = daily['ts_code']
    new_df['open'] = daily['open']
    new_df['close'] = daily['close']
    new_df['high'] = daily['high']
    new_df['low'] = daily['low']
    new_df['pre_close'] = daily['pre_close']
    new_df['volume'] = daily['vol']
    new_df['money'] = daily['amount']
    for i in range(len(new_df)):
        new_df.loc[i,'name'] = get_name(daily.loc[i,'ts_code'].split('.')[0])
        # 如果成交量不为0，说明没有停牌
        if new_df.loc[i,'volume'] !=0:
            new_df.loc[i,'paused'] = 0
            new_df.loc[i,'avg'] = new_df.loc[i,'money']/new_df.loc[i,'volume']
        else:
            new_df.loc[i,'paused'] = 1
            new_df.loc[i,'avg'] = 0.0
        new_df.loc[i,'high_limit'] = new_df.loc[i,'pre_close']*1.1
        new_df.loc[i,'low_limit'] = new_df.loc[i,'pre_close']*0.9
    return new_df


# 对某一天的ts数据进行处理为符合我们放入数据库的格式
def ts_cope(daily):
    # 删除北交所股票
    index_list = []
    for index,row in daily.iterrows():
        if row['ts_code'][-2:] == 'BJ':
            index_list.append(index)
    daily = daily.drop(index_list,axis=0)
    # 重排索引，否则没法使用for循环调整属性
    daily = daily.reset_index(drop=True)
    # 处理股票代码,将其转换为SZ-XSHE/SH-XSHG
    codes = daily['ts_code']
    dic_code = {'SZ':'XSHE','SH':'XSHG'}
    new_codes = []
    for code in codes:
        code_last = code[-2:]
        new_code = code[:-3] + '.' + dic_code[code_last]
        new_codes.append(new_code)
    daily.loc[:,'ts_code'] = new_codes
    # 将成交量(手)和金额还有日期进行转换
    for i in range(len(daily)):
        daily.iloc[i,1] = datetime.datetime.strptime(daily.iloc[i,1],"%Y%m%d")
        daily.iloc[i,9] = daily.iloc[i,9]*100
        daily.iloc[i,10] = daily.iloc[i,10]*1000
    # 转换为数据库模式
    new_df = todatabase(daily)
    return new_df

# 交易日历
# trade_cal = pro.trade_cal(
#     start_date='20230905', end_date='20230905', is_open='1'
# )['cal_date'].tolist()
# # 按照交易日历取每个交易日的数据
# for day in trade_cal:
#     daily = pro.daily(start_date=day, end_date=day)
#     new_df = ts_cope(daily=daily)
#     output = StringIO()
#     new_df.to_csv(output, sep='\t', index=False, header=False)
#     output1 = output.getvalue()
#     cur.copy_from(StringIO(output1),'daily_stock',columns=('date','code','name',"open", "close", "high", "low", "turnover", "volume", "avg","high_limit","low_limit","pre_close","paused"))
#     conn.commit()

@execute_at_time(16,30,21,30)
def update():
    current = datetime.datetime.now().strftime("%Y%m%d")
    # 依据tushare来判断是否为交易日,如果不是交易日直接任务完成
    df = pro.query('trade_cal', start_date = current, end_date = current, is_open = '1')
    if len(df) == 0:
        return True
    # 打印之前存储最后一天的数据进行展示检查
    sql1 = 'SELECT * FROM daily_stock where code = \'000001.XSHE\' order by date desc limit 1'
    cur.execute(sql1)
    data = cur.fetchone()
    if data != None:
        print('*'*35,"这是一条对于前一日平安银行的行情数据的打印提示信息",'*'*35)
        print(data)
        # 执行更新语句并进行检查
        print("-"*25,"updating",'-'*25)
        daily = pro.daily(start_date=current, end_date=current)
        new_df = ts_cope(daily=daily)
        output = StringIO()
        new_df.to_csv(output, sep='\t', index=False, header=False)
        output1 = output.getvalue()
        cur.copy_from(StringIO(output1),'daily_stock',columns=('date','code','name',"open", "close", "high", "low", "turnover", "volume", "avg","high_limit","low_limit","pre_close","paused"))
        conn.commit()
        print("-"*25,"finish",'-'*25)
        sql2 = 'SELECT * FROM daily_stock where code = \'000001.XSHE\' and  date = \'%s\''%(current)
        cur.execute(sql2)
        data = cur.fetchone()
        # 如果出现今天的数据没有导入则需要人工检查是否出错或者是否休市
        if data == None:
            print("不存在数据！可能是输入错误或者今天休市")
            return False
        return True
    else:
        print("昨天的数据未查询到，请检查！")
        return False
while True:
    flag = update()
    print(flag)
    # 如果发现没有运行任务，半小时后检查
    if flag == False or flag == None:
        time.sleep(1800)
    # 如果发现当日已经运行，半天后再检查，并且把flag重置
    else:
        time.sleep(36000)
        flag = False