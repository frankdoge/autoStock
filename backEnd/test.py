from jqdatasdk import *
import time
import easyquotation
import easytrader
import pandas as pd
import redis
import pywinauto
import psycopg2
import datetime
import time
import re
import json
auth('15707956978','CSUbigdata123')
# 在本地连接redis
pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool, decode_responses=True)
# 连接postgresql数据库
conn = psycopg2.connect(database='stock', user='postgres',password='csubigdata',host='localhost',port='5432')
cur = conn.cursor()
# 调用easytrader下单程序
bigdata1 = easytrader.use('universal_client')#定义使用universal_client

def get_ma(md1,md2):
    current_day = datetime.date.today()
    DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
    result = r.get(DateKey)
    if result == None:
        # 从数据库中获得上一个交易日的所有股票代码
        df = pd.DataFrame(columns=['name','code','close'])
        yesterday = trade_cal[1]

        sql_code = 'select code from daily_stock where date = \'%s\''%(yesterday)
        cur.execute(sql_code)
        result = cur.fetchall()
        code_list = [result[i][0].split(' ')[0] for i in range(len(result))]
        # 起始日期的下标刚好是md2，因为下标0代表今天,放入data中
        start = trade_cal[md2]
        end = trade_cal[1]
        sql = 'select date,name,code,close from daily_stock where date >= \'%s\' and date<= \'%s\''%(start,end)
        cur.execute(sql)
        data = cur.fetchall()
        data = pd.DataFrame(data,columns=['date','name','code','close'])
        data['name'] = data['name'].str.strip()
        data['code'] = data['code'].str.strip()
        # 进行计算
        for code in code_list[0:10]:
            info = data[data['code']==code]
            # 将该股票的dataframe按时间降序排列
            new_info = info.sort_values(by='date',inplace=False,ascending=False)
            val_ma1 = new_info.iloc[:md1,3].mean()
            val_ma2 = new_info.iloc[:,3].mean()
            # 满足条件则插入dateframe
            if val_ma1 > val_ma2:
                name = new_info.iloc[0,1]
                close = float(new_info.iloc[0,3])
                indexsize = df.index.size
                df.loc[indexsize] = [name,code,close]
                df.index = df.index + 1
        result = df.to_json(orient="records")
        r.set(DateKey,result)
    return result


def denormalize_code(code:str):
    dic_code = {'XSHE':'sz','XSHG':'sh'}
    code_orig = code[7:]
    code_new = dic_code[code_orig] + code[:6]
    return code_new
quotation = easyquotation.use('sina')
try:
    bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
except:
    bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
print("init finish")
# return_info = bigdata1.buy('000802',6.00,100)
    # 获取观望中/已买入所有订单
sql = 'select id,code,state,updated_time,stra_param_buy,stra_param_sell,amount from stockorder where state = 0 or state = 1'
cur.execute(sql)
order = cur.fetchall()
order_df = pd.DataFrame(order, columns=['id','code','state','updated_time','stra_param_buy','stra_param_sell','amount'])
# 存储操作记录用于更新数据库
update_dict = {}
# 获取实时数据
data = quotation.market_snapshot(prefix=True)

for index,row in order_df.iterrows():
    code = row['code']
    id = row['id']
    amount = row['amount']*100
    code_denomalized = denormalize_code(code)
    # 如果传入数据没有这个股票的实时信息，则略过
    if code_denomalized not in data.keys():
        continue
    else:
        # 捕获当前的价格
        now = data[code_denomalized]['now']
        # 如果状态为未买入
        if row['state'] == 0:
            stra_buy = re.split(r"[ ,]",row['stra_param_buy'])
            if stra_buy[0].split(":")[0] == 'price':
                # 如果是price策略，则判断
                target_price = float(stra_buy[0].split(":")[1])
                if now <= target_price:
                    # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                    return_info = bigdata1.buy(code[:6],target_price,amount)
                    print(stra_buy[0],return_info)
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id
            elif stra_buy[0].split(":")[0] == 'percent':
                # 如果是percent策略，则判断买入价格是否到达对应百分比，默认买入是1-percent
                target_percent = stra_buy[0].split(":")[1]
                target_percent = float(target_percent.split('%')[0])
                target_price = float(stra_buy[1].split(":")[1])
                target_price = target_price*(1-0.01*target_percent)
                if now <= target_price:
                    try:
                        # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                        return_info = bigdata1.buy(code[:6],target_price,amount)
                        print(stra_buy[0],return_info)
                    except IOError:
                        print("休市或者订单信息有误")
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id
            elif stra_buy[0].split(":")[0] == 'ma1':
                # 如果是MA策略，则判断对应股票是否在redis中
                current_day = datetime.date.today()
                md1 = int(stra_buy[0].split(":")[1])
                md2 = int(stra_buy[1].split(":")[1])
                DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
                result = r.get(DateKey)
                if result == None:
                    result = get_ma(md1=md1,md2=md2)

                result = json.loads(result)
                result = pd.DataFrame(result,columns=['name','code','close'])
                # 返回的dataframe中的全都是md1>md2的，则如果股票代码在其中就可以买入
                if not result[result['code'].str.contains(code)].empty:
                    return_info = bigdata1.buy(code[:6],now,amount)
                    print(stra_buy[0],return_info)
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id

        else:
            # 如果状态为已买入,需要检查一下是否当天买入
            if row['updated_time'] == datetime.datetime.today:
                continue
            stra_sell = re.split(r"[ ,]",row['stra_param_sell'])
            if stra_sell[0].split(':')[0] == 'price':
                # 如果是price策略，则判断
                target_price = float(stra_sell[0].split(':')[1])
                if now >= target_price:
                    # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                    return_info = bigdata1.sell(code[:6],target_price,amount)
                    print(stra_buy[0],return_info)
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id
            elif stra_sell[0].split(':')[0] == 'percent':
                # 如果是percent策略，则判断
                target_percent = stra_sell[0].split(":")[1]
                target_percent = float(target_percent.split('%')[0])
                target_price = float(stra_sell[1].split(":")[1])
                target_price = target_price*(1+0.01*target_percent)
                if now >= target_price:
                    # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                    return_info = bigdata1.sell(code[:6],target_price,amount)
                    print(stra_buy[0],return_info)
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id
            elif stra_sell[0].split(':')[0] == 'ma1':
                # 如果是MA策略，则判断
                # 如果是MA策略，则判断对应股票是否在redis中
                current_day = datetime.date.today()
                md1 = int(stra_sell[0].split(":")[1])
                md2 = int(stra_sell[1].split(":")[1])
                DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
                result = r.get(DateKey)
                if result == None:
                    result = get_ma(md1=md1,md2=md2)

                result = json.loads(result)
                result = pd.DataFrame(result,columns=['name','code','close'])
                # 返回的dataframe中的全都是md1>md2的，则如果股票代码不在其中就可以卖出
                if not result[result['code'].str.contains(code)].empty:
                    return_info = bigdata1.sell(code[:6],now,amount)
                    print(stra_buy[0],return_info)
                    # 已经存在下单单号
                    if 'entrust_no' in return_info.keys():
                        update_dict[return_info['entrust_no']] = id