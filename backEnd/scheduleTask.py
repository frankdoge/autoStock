import datetime
import pandas as pd
from Utils import *
import re
import json
import threading
# 使用全局变量保存需要检查的股票代码,分别对应id和卖出price

updateCode = {}
updatePrice = {}

# 全局变量保存今日
today = str(datetime.datetime.now().date())

# 因为两个定时任务都会对保存的委托股票代码进行操作，需要进行互斥锁
mutex = threading.Lock()
'''
在工作日使用cron表达式使每个工作日每半个小时检查一次委托是否成交
逻辑是遍历我的持仓检查是否有当日买入和当日卖出
'''
@scheduler.scheduled_job('cron',hour='9-11,13-15',minute='*/8', day_of_week ='0-4')
async def test_cron():
    mutex.acquire()
    # 检查是否为开盘时间
    if not isOpen():
        mutex.release()
        return
    # 如果时间进入到下一天，置空当日成交变量
    global updatePrice
    global updateCode
    global today
    if today != str(datetime.datetime.now().date()):
        updateCode.clear()
        updatePrice.clear()
        today = str(datetime.datetime.now().date())
        mutex.release()
        return
    try:
        print('-'*20,'start check entrust','-'*20)
        flag = start()
        if flag == "success":
            positionInfo = bigdata1.position
            print("持仓信息",positionInfo)
            print("-"*60)
            print("委托中的股票代码和id",updateCode)
            for element in positionInfo:
                if element['当日买入'] !=0 or element['当日卖出'] !=0:
                    # 判断是否在委托订单中
                    if element['证券代码'] in updateCode:
                        if element['当日买入'] !=0:
                            price = element['成本价1']
                            amount = element['当日买入']/100
                            state = 1
                            id = updateCode[element['证券代码']]
                            updated_time = str(datetime.datetime.now().date())
                            sqlUpdate = 'update stockorder set state =%d,updated_time = \'%s\',amount = %d,buy_price = %f where id = %d' %(state,updated_time,amount,price,id)
                            print(sqlUpdate)
                            cur.execute(sqlUpdate)
                            conn.commit()
                            del updateCode[element['证券代码']]
                        else:
                            amount = element['当日卖出']/100
                            state = 2
                            id = updateCode[element['证券代码']]
                            price = updatePrice[element['证券代码']]
                            updated_time = str(datetime.datetime.now().date())
                            sqlUpdate  = 'update stockorder set state =%d,updated_time = \'%s\',amount = %d,sell_price = %f where id = %d' %(state,updated_time,amount,price,id)
                            print(sqlUpdate)
                            cur.execute(sqlUpdate)
                            conn.commit()
                            del updateCode[element['证券代码']]
                            del updatePrice[element['证券代码']]
        print('-'*20,'finish check entrust','-'*20)
        mutex.release()
    except:
        mutex.release()
        return "订单更新异常，后台正在维护"


'''
类似于心跳进程来保持与同花顺端的连接,否则会发生断开
'''
@scheduler.scheduled_job('cron',hour='*/1',minute='5',second='40')
async def heart_beats():
    start()



'''
在工作日使用cron表达式使每个工作日(不含特殊节假日)9:00-11:59/13:00-15:59每五分钟执行的定时任务
使用的逻辑是利用委托单号来在当日成交过后来判断某只股票是否已成交
'''
# @scheduler.scheduled_job('cron',hour='9-11,13-15',minute='*/5', day_of_week ='0-4')
# async def cron_job_entrustNum():
#     # 执行任务的内容，例如打印当前时间
#     if not isOpen():
#         return
#     flag = start()
#     if flag == 'success':
#         # 获取观望中/已买入所有订单
#         sql = 'select id,code,state,updated_time,stra_param_buy,stra_param_sell,amount from stockorder where state = 0 or state = 1'
#         cur.execute(sql)
#         order = cur.fetchall()
#         order_df = pd.DataFrame(order, columns=['id','code','state','updated_time','stra_param_buy','stra_param_sell','amount'])
#         # 存储操作记录用于更新数据库
#         global update_dict 
#         # 获取实时数据
#         data = quotation.market_snapshot(prefix=True)

#         for index,row in order_df.iterrows():
#             code = row['code']
#             id = row['id']
#             amount = row['amount']*100
#             code_denomalized = denormalize_code(code)
#             # 如果传入数据没有这个股票的实时信息，则略过
#             if code_denomalized not in data.keys():
#                 continue
#             else:
#                 # 捕获当前的价格
#                 now = data[code_denomalized]['now']
#                 # 如果状态为未买入
#                 if row['state'] == 0:
#                     stra_buy = re.split(r"[ ,]",row['stra_param_buy'])
#                     if stra_buy[0].split(":")[0] == 'price':
#                         # 如果是price策略，则判断
#                         target_price = float(stra_buy[0].split(":")[1])
#                         if now <= target_price:
#                             # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
#                             return_info = bigdata1.buy(code[:6],target_price,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id
#                     elif stra_buy[0].split(":")[0] == 'percent':
#                         # 如果是percent策略，则判断买入价格是否到达对应百分比，默认买入是1-percent
#                         target_percent = stra_buy[0].split(":")[1]
#                         target_percent = float(target_percent.split('%')[0])
#                         target_price = float(stra_buy[1].split(":")[1])
#                         target_price = target_price*(1-0.01*target_percent)
#                         if now <= target_price:
#                             # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
#                             return_info = bigdata1.buy(code[:6],target_price,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id
#                     elif stra_buy[0].split(":")[0] == 'ma1':
#                         # 如果是MA策略，则判断对应股票是否在redis中
#                         current_day = datetime.date.today()
#                         md1 = int(stra_buy[0].split(":")[1])
#                         md2 = int(stra_buy[1].split(":")[1])
#                         DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
#                         result = r.get(DateKey)
#                         if result == None:
#                             result = get_ma(md1=md1,md2=md2)

#                         result = json.loads(result)
#                         result = pd.DataFrame(result,columns=['name','code','close'])
#                         # 返回的dataframe中的全都是md1>md2的，则如果股票代码在其中就可以买入
#                         if not result[result['code'].str.contains(code)].empty:
#                             return_info = bigdata1.buy(code[:6],now,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id

#                 else:
#                     # 如果状态为已买入,需要检查一下是否当天买入
#                     if row['updated_time'] == datetime.datetime.today:
#                         continue
#                     stra_sell = re.split(r"[ ,]",row['stra_param_sell'])
#                     if stra_sell[0].split(':')[0] == 'price':
#                         # 如果是price策略，则判断
#                         target_price = float(stra_sell[0].split(':')[1])
#                         if now >= target_price:
#                             # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
#                             return_info = bigdata1.sell(code[:6],target_price,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id
#                     elif stra_sell[0].split(':')[0] == 'percent':
#                         # 如果是percent策略，则判断
#                         target_percent = stra_sell[0].split(":")[1]
#                         target_percent = float(target_percent.split('%')[0])
#                         target_price = float(stra_sell[1].split(":")[1])
#                         target_price = target_price*(1+0.01*target_percent)
#                         if now >= target_price:
#                             # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
#                             return_info = bigdata1.sell(code[:6],target_price,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id
#                     elif stra_sell[0].split(':')[0] == 'ma1':
#                         # 如果是MA策略，则判断
#                         # 如果是MA策略，则判断对应股票是否在redis中
#                         current_day = datetime.date.today()
#                         md1 = int(stra_sell[0].split(":")[1])
#                         md2 = int(stra_sell[1].split(":")[1])
#                         DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
#                         result = r.get(DateKey)
#                         if result == None:
#                             result = get_ma(md1=md1,md2=md2)

#                         result = json.loads(result)
#                         result = pd.DataFrame(result,columns=['name','code','close'])
#                         # 返回的dataframe中的全都是md1>md2的，则如果股票代码不在其中就可以卖出
#                         if not result[result['code'].str.contains(code)].empty:
#                             return_info = bigdata1.sell(code[:6],now,amount)
#                             print(stra_buy[0],return_info)
#                             # 已经存在下单单号
#                             if 'entrust_no' in return_info.keys():
#                                 update_dict[return_info['entrust_no']] = id
#         # 根据当日成交来判断update_info中的信息是否应该更新
#         print("get today_trades")
#         trade_info = bigdata1.today_trades
#         trade_info = pd.DataFrame(trade_info)
#         print(trade_info)
#         del_key = []
#         for key,value in update_dict.items():
#             res = trade_info[trade_info['委托序号']==key]
#             if res.empty:
#                 continue
#             # 当前记录已经成交
#             else:
#                 del_key.append(key)
#                 state = 0
#                 price = res['成交价格']
#                 updated_time = res['成交时间']
#                 updated_time = datetime.datetime.strptime(updated_time,"%Y-%m-%d")
#                 updated_time = datetime.date(updated_time)
#                 if res['买卖标志'] == '买入':
#                     state = 1
#                     sql = 'update stockorder set state =\'%d\',updated_time = \'%s\', buy_price = \'%f\' where id = \'%d\'' %(state,updated_time,price,id)
#                     cur.execute(sql)
#                 else:
#                     state = 2
#                     sql = 'update stockorder set state =\'%d\',updated_time = \'%s\', sell_price = \'%f\' where id = \'%d\'' %(state,updated_time,price,id)
#                     cur.execute(sql)
#         # 删去已更新的信息
#         for element in del_key:
#             del update_dict[element]
#         print("finish updating stockorder")   


'''
在工作日使用cron表达式使每个工作日(不含特殊节假日)9:00-11:59/13:00-15:59每五分钟执行的定时任务
使用的逻辑是根据数据库中的股票订单来判断是否符合条件进行买入/卖出
如果满足条件则放入全局变量updateCode = {'code':'id'}的形式/updatePrice = {'code':'price'}
'''
@scheduler.scheduled_job('cron',hour='9-11,13-15',minute='*/15', day_of_week ='0-4')
async def cron_job_position():
    mutex.acquire()
    # 先检查当前是否为开盘时间
    if not isOpen():
        mutex.release()
        return
    # 这里不调用start是希望在启动交易端前更新交易日历
    flag = updateTradeCal()
    if flag == 'success':
        print('-'*20,'start check strategy','-'*20)
        # 获取观望中/已买入所有订单
        sql = 'select id,code,state,updated_time,stra_param_buy,stra_param_sell,amount from stockorder where state = 0 or state = 1'
        cur.execute(sql)
        order = cur.fetchall()
        order_df = pd.DataFrame(order, columns=['id','code','state','updated_time','stra_param_buy','stra_param_sell','amount'])
        # 获取实时数据
        data = quotation.market_snapshot(prefix=True)

        global updatePrice
        global updateCode

        for index,row in order_df.iterrows():
            code = row['code']
            id = row['id']
            amount = row['amount']*100
            code_denomalized = denormalize_code(code)
            # 如果传入数据没有这个股票的实时信息，则略过
            if code_denomalized not in data.keys():
                continue
            # 如果当前股票代码和对应id是在委托中，则不进行策略检测
            if code[:6] in updateCode:
                if updateCode[code[:6]] == id:
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
                            print(return_info)
                            updateCode[code[:6]] = id
                            
                            # buyUpdate(code[:6],id)
                    elif stra_buy[0].split(":")[0] == 'percent':
                        # 如果是percent策略，则判断买入价格是否到达对应百分比，默认买入是1-percent
                        target_percent = stra_buy[0].split(":")[1]
                        target_percent = float(target_percent.split('%')[0])
                        target_price = float(stra_buy[1].split(":")[1])
                        target_price = target_price*(1-0.01*target_percent)
                        if now <= target_price:
                            # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                            return_info = bigdata1.buy(code[:6],target_price,amount)
                            print(return_info)
                            
                            updateCode[code[:6]] = id

                            # buyUpdate(code[:6],id)
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
                            print(return_info)
                            
                            updateCode[code[:6]] = id
                            
                            # buyUpdate(code[:6],id)
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
                            print(return_info)
                            
                            updateCode[code[:6]] = id
                            updatePrice[code[:6]] = target_price

                            # sellUpdate(code[:6],id,target_price)
                    elif stra_sell[0].split(':')[0] == 'percent':
                        # 如果是percent策略，则判断
                        target_percent = stra_sell[0].split(":")[1]
                        target_percent = float(target_percent.split('%')[0])
                        target_price = float(stra_sell[1].split(":")[1])
                        target_price = target_price*(1+0.01*target_percent)
                        if now >= target_price:
                            # 下单会返回一个委托单号{'entrust_no':},如果成交/错误则返回{'message':}
                            return_info = bigdata1.sell(code[:6],target_price,amount)
                            print(return_info)
                            
                            updateCode[code[:6]] = id
                            updatePrice[code[:6]] = target_price
                            
                            # sellUpdate(code[:6],id,target_price)
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
                            print(return_info)
                            
                            updateCode[code[:6]] = id
                            updatePrice[code[:6]] = now

                            # sellUpdate(code[:6],id,now)
        print("下委托定时任务时的股票代码",updateCode)
        print('-'*20,'finish check strategy','-'*20)
    mutex.release()