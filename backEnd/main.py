import easyquotation
import json
import uvicorn
import pandas as pd
import numpy as np
from typing import Optional
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from Utils import *
from scheduleTask import *



# 定义传入的请求体参数,为两个均线长短值
class MA(BaseModel):
    md1: int
    md2: int
    description: Optional[str] = "input two int value to calcu"

class stock(BaseModel):
    name:str
    code:str
    close:float



# 启动scheduler
@app.on_event("startup")
async def startup_event():
    print("FastApi服务启动")
    # 启动定时任务
    scheduler.start()



# 获得股票代码列表
code = get_all_stock_code()

@app.get("/app/start",summary="启动检测")
async def start_judge():
    '''
    启动服务判断，首先将后端的下单客户端进行拉取选定,防止pywinauto出现异常
    并且更新交易日历,每天一次,如果变化了则更新否则pass
    - param 无参数
    - return 返回"success"字符串
    '''
    start()
    global trade_cal
    current = datetime.datetime.now()
    before = current + datetime.timedelta(days=-90)
    before = before.strftime("%Y%m%d")
    current = current.strftime("%Y%m%d")
    # 如果还是今天则不更新
    if current == trade_cal[0]:
        pass
    # 更新交易日历
    else:
        trade_cal = pro.trade_cal(
        start_date= before, end_date=current, is_open='1'
        )['cal_date'].tolist()
    return "success"


@app.get("/app/",summary="根目录进行交易端选定")
async def start_process():
    '''
    启动首页
    将后端的下单客户端进行拉取选定
    - param 无传参
    - return 返回"hello for fastapi"字符串
    '''
    start()
    return "hello for fastapi"



@app.post("/app/getStrategyInfo",summary="全部取出到dataframe处理")
async def getStrategyInfo(item :MA):
    '''
    满足策略股票的选定,从数据库选择,直接将所有股票的前md2天信息转换为dataframe处理
    获取两个均线数据来进行股票选择
    - param item: 请求体格式，包含两个int即md1和md2
    - return 短期均线值大于长期均线值的股票
    {\n
        name:"" ->str,\n
        code:"" ->str,\n
        close:"" ->float,\n
    }
    '''
    md1 = item.md1
    md2 = item.md2
    current_day = datetime.date.today()
    DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
    result = r.get(DateKey)
    if result == None:
        # 判断程序是否正常启动
        flag = await start_judge()
        if flag == 'success':
            # 从redis中获得上一个交易日的所有股票代码
            df = pd.DataFrame(columns=['name','code','close'])
            yesterday = trade_cal[1]
            # DateKey = yesterday + ":allstock"
            # result = r.get(DateKey)
            # result = json.loads(result)
            # code_list = [result[i]['code'].split(' ')[0] for i in range(len(result))]

            #从postgresql中拿
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
            for code in code_list:
                info = data[data['code']==code]
                # 将该股票的dataframe按时间降序排列
                new_info = info.sort_values(by='date',inplace=False,ascending=False)
                val_ma1 = info.iloc[0:md1,3].mean()
                val_ma2 = info.iloc[:,3].mean()
                # 满足条件则插入dateframe
                if val_ma1 > val_ma2:
                    name = new_info.iloc[0,1]
                    close = float(new_info.iloc[0,3])
                    indexsize = df.index.size
                    df.loc[indexsize] = [name,code,close]
                    df.index = df.index + 1
            result = df.to_json(orient="records")
            r.set(DateKey,result)
        else:
            return "程序出现错误"
    return result

@app.post("/app/addOrder",summary="根据客户端请求将选中股票放入数据库")
async def addOrder(request :Request):
    '''
    后端通过获取客户端传来的request从而将数据加入观望台再返回给客户端,应用于添加股票到观望台和修改卖出策略的场景
    基本逻辑是将信息放入postgresql数据库的股票订单对象中\n
    还需要判断一下request的body中是否含有id字段,如果含有则是修改卖出策略
    - param request的body中应该含有股票代码、名称、价格、买入/卖出条件、买入数量和策略参数
    {\n
        id:"" ->int(optional),\n
        code:"" ->str,\n
        name:"" ->str,\n
        price:"" ->float,\n
        straNameBuy:"" ->int,\n
        straNameSell:"" ->int,\n
        straParamBuy ->dict,\n
        straParamSell ->dict,\n
    }
    - return "insert finish"字符串
    '''
    body = await request.body()
    # 将收到的body解码转换为字典格式
    body = body.decode(encoding='utf-8')
    data = json.loads(body)
    result = {}
    if not data.get("id"):
        # 如果是添加股票订单
        result['code'] = data['code'].strip()
        result['name'] = data['codeName'].strip()
        result['price'] = data['price']
        result['buy_percent'] = data['straNameBuy']
        result['sell_percent'] = data['straNameSell']
        data['straParamBuy'] = eval(data['straParamBuy'])
        data['straParamSell'] = eval(data['straParamSell'])
        # 表示当前的订单状态在观望中
        result['state'] = 0
        result['created_time'] = datetime.date.today()
        result['amount'] = data['amount']
        # 根据不同的策略保存策略参数
        if result['buy_percent'] == 1:
            result['stra_param_buy'] = "price:"+ data['straParamBuy']['price']
        elif result['buy_percent'] == 2:
            result['stra_param_buy'] = 'percent:' + data['straParamBuy']['percent'] + ",price:" + data['straParamBuy']['price']
        else:
            result['stra_param_buy'] = "ma1:" + data['straParamBuy']['ma1'] + ',ma2:' + data['straParamBuy']['ma2']+ ",mode:" + data['straParamBuy']['mode']
        # 卖出策略参数与买入策略参数类似
        if result['sell_percent'] == 1:
            result['stra_param_sell'] = "price:"+ data['straParamSell']['price']
        elif result['sell_percent'] == 2:
            result['stra_param_sell'] = 'percent:' + data['straParamSell']['percent'] + ",price:" + data['straParamSell']['price']
        else:
            result['stra_param_sell'] = "ma1:" + data['straParamSell']['ma1'] + ',ma2:' + data['straParamSell']['ma2']+ ",mode:" + data['straParamSell']['mode']
        sql = f"INSERT INTO stockorder (code,name,price,buy_percent,sell_percent,state,created_time,amount,stra_param_buy,stra_param_sell) VALUES ('{result['code']}','{result['name']}','{result['price']}','{result['buy_percent']}','{result['sell_percent']}','{result['state']}','{result['created_time']}','{result['amount']}','{result['stra_param_buy']}','{result['stra_param_sell']}')"
        print(sql)
        cur.execute(sql)
        conn.commit()
    else:
        # 如果是修改卖出策略
        result['id'] = data['id']
        result['sell_percent'] = data['straNameSell']
        data['straParamSell'] = eval(data['straParamSell'])
        if result['sell_percent'] == 1:
            result['stra_param_sell'] = "price:"+ data['straParamSell']['price']
        elif result['sell_percent'] == 2:
            result['stra_param_sell'] = 'precent:' + data['straParamSell']['percent'] + ",price:" + data['straParamSell']['price']
        else:
            result['stra_param_sell'] = "ma1:" + data['straParamSell']['ma1'] + ',ma2:' + data['straParamSell']['ma2']+ ",mode:" + data['straParamSell']['mode']
        sqlUpdate = 'update stockorder set sell_percent = %d, stra_param_sell = \'%s\' where id = %d'%(result['sell_percent'],result['stra_param_sell'],result['id'])
        print(sqlUpdate)
        cur.execute(sqlUpdate)
        conn.commit()
    return "insert finish"

@app.post("/app/getPositionHasBuy",summary="获取订单持仓信息")
async def getPositionHasBuy(request :Request):
    '''
    根据订单状态将观望中/已买入/已卖出订单返回到观望台/我的持仓/完成订单中
    - param request的body中应该含有一个int代表订单状态,从而在数据库中查询
    0表示观望中 1表示已买入 2表示已卖出
    - return 返回股票订单对象
    {\n
        id:"" ->int,\n
        code:"" ->str,\n
        name:"" ->str,\n
        price:"" ->float,\n
        buy_percent:"" ->int,\n
        sell_percent:"" ->int,\n
        state:"" ->int,\n
        created_time:"" ->date,\n
        updated_time:"" ->date,\n
        buy_price:"" ->float,\n
        sell_price:"" ->float,\n
        amount:"" ->int,\n
        stra_param_buy ->dict,\n
        stra_param_sell ->dict,\n
    }
    '''
    body = await request.body()
    # 将收到的body解码转换为字典格式
    body = body.decode(encoding='utf-8')
    data = json.loads(body)
    data = data['state']
    df = pd.DataFrame(columns=['id','code','name','price','buy_percent','sell_percent','state','created_time','updated_time','buy_price','sell_price','amount','stra_param_buy','stra_param_sell'])
    # 返回的是观望中的订单对象
    if data == 0:
        quotation = easyquotation.use('sina')
        tickData = quotation.market_snapshot(prefix=True)
        sql = 'select id,code from stockorder'
        cur.execute(sql)
        dbData = cur.fetchall()
        dbData = pd.DataFrame(dbData,columns=['id','code'])
        for i in range(len(dbData)):
            # 依次查找每一个股票的实时价格数据
            code = denormalize_code(dbData.loc[i,'code'])
            for k,v in tickData.items():
                if code in k:
                    sqlUpdate = 'update stockorder set price = %f where id =%d'%(v['now'],dbData.loc[i,'id'])
                    cur.execute(sqlUpdate)
                    conn.commit()
        sql = 'select * from stockorder where state = 0'
        cur.execute(sql)
        data = cur.fetchall()
        # 将查询结果转化为dataframe
        data = pd.DataFrame(data,columns=df.columns)
        print(data)
    elif data == 1:
        quotation = easyquotation.use('sina')
        tickData = quotation.market_snapshot(prefix=True)
        sql = 'select id,code from stockorder'
        cur.execute(sql)
        dbData = cur.fetchall()
        dbData = pd.DataFrame(dbData,columns=['id','code'])
        for i in range(len(dbData)):
            # 依次查找每一个股票的实时价格数据
            code = denormalize_code(dbData.loc[i,'code'])
            for k,v in tickData.items():
                if code in k:
                    sqlUpdate = 'update stockorder set price = %f where id =%d'%(v['now'],dbData.loc[i,'id'])
                    cur.execute(sqlUpdate)
                    conn.commit()
        sql = 'select * from stockorder where state = 1'
        cur.execute(sql)
        data = cur.fetchall()
        # 将查询结果转化为dataframe
        data = pd.DataFrame(data,columns=df.columns)
        print(data)
    else:
        sql = 'select * from stockorder where state = 2'
        cur.execute(sql)
        data = cur.fetchall()
        # 将查询结果转化为dataframe
        data = pd.DataFrame(data,columns=df.columns)
        print(data)
    return data.to_json(orient="records")

@app.post("/app/getStrategyInfoBase",summary="从数据库中一次只处理一支股票")
async def getStrategyInfoBase(item :MA):
    
    '''
    满足策略股票的选定
    获取两个均线数据来进行股票选择,从数据库中获得,按照股票代码一次查询计算一只股票,参数与返回值和getStrategyInfo一致
    - param item: 请求体格式，包含两个int即md1和md2
    - return 短期均线值大于长期均线值的股票
    '''
    md1 = item.md1
    md2 = item.md2
    current_day = datetime.date.today()
    DateKey = current_day.strftime("%Y%m%d")+":"+str(md1)+str(md2)
    result = r.get(DateKey)
    if result == None:
        # 判断程序是否正常启动
        flag = await start_judge()
        if flag == 'success':
            # 从redis中获得上一个交易日的所有股票代码
            df = pd.DataFrame(columns=['name','code','close'])
            yesterday = trade_cal[1]
            DateKey = yesterday + ":allstock"
            result = r.get(DateKey)
            result = json.loads(result)
            code_list = [result[i]['code'].split(' ')[0] for i in range(len(result))]
            # 进行计算
            for code in code_list:
                # 数据库中查询转入numpy计算
                sql_ma1 = 'select name,close from daily_stock where code = \'%s\' order by date desc limit %d' %(code,md1)
                cur.execute(sql_ma1)
                data = cur.fetchall()
                # 记录这支股票的名字和收盘价(上一日)
                name = data[0][0].split(" ")[0]
                close = float(data[0][1])
                data = np.array(data)
                data = data[:,1]
                val_ma1 = float(np.mean(data))

                sql_ma2 = 'select name,close from daily_stock where code = \'%s\' order by date desc limit %d' %(code,md2)
                cur.execute(sql_ma2)
                data = cur.fetchall()
                data = np.array(data)
                data = data[:,1]
                val_ma2 = float(np.mean(data))
                # 满足条件则插入dateframe
                if val_ma1 > val_ma2:
                    indexsize = df.index.size
                    df.loc[indexsize] = [name,code,close]
                    df.index = df.index + 1
            result = df.to_json(orient="records")
            r.set(DateKey,result)
        else:
            return "程序出现错误"
    return result


@app.post("/app/revokeOrder",summary="撤销观望台中的股票订单")
async def revokeOrder(request: Request):
    '''
    根据观望台中的股票代码来取消
    - param 传入股票id
    {\n
        id:"" ->int\n 
    }
    '''
    body = await request.body()
    # 将收到的body解码转换为字典格式
    body = body.decode(encoding='utf-8')
    data = json.loads(body)
    id = data['id']
    sql = 'delete from stockorder where id = %d' %(id)
    cur.execute(sql)
    conn.commit()
    return "撤销成功"
@app.get("/app/balanceInfo",summary="获取资金信息")
async def balanceInfo():
    '''
    获取当前资金信息，每天调用一次，为了节约开销我们存入redis
    - param 无参数
    - return 含有四个float的参数,分别代表资金余额,可用金额,可取金额,总资产
    '''
    
    current_day = datetime.date.today()
    TodayKey = current_day.strftime("%Y%m%d")+":position"
    result = r.get(TodayKey)
    # 如果redis中没有这个键，那么我们调度一次easytrader中的持仓信息
    if result == None:
        # 判断当前是否启动了程序
        flag = await start_judge()
        if flag == 'success':
            result = bigdata1.balance
        r.set(TodayKey,json.dumps(result))
    return result


@app.get("/app/impliedVolatility",summary="返回隐含波动率")
async def impliedVolatility():
    '''
    根据期权论坛上的实时更新数据进行返回，使用的是selenium+beautifulsoup进行动态网页爬取
    - param 无传参
    - return 字符串类型的隐含波动率
    '''
    driver = webdriver.Edge("C:\Program Files (x86)\Microsoft\Edge\Application\msedgedriver.exe")
    driver.get('https://1.optbbs.com/s/vix.shtml')
    td = driver.find_elements(By.ID,"last")[0]
    res = td.text
    driver.quit()
    return res
    
@app.get("/app/allStock",summary='获取当日所有股票信息(前一日数据库中的信息)')
async def allStock():
    '''
    获取所有股票的信息，每天调用一次，为了节约开销我们存入redis
    - param 无传参
    - return json格式的所有股票的列表，每个元素含{名称，代码，收盘价}
    '''
    current_day = datetime.date.today()
    TodayKey = current_day.strftime("%Y%m%d")+":allstock"
    result = r.get(TodayKey)
    # 如果redis中没有这个键，那么我们访问一次postgres数据库
    if result == None:
        # 判断当前是否启动了程序
        flag = await start_judge()
        if flag == 'success':
            result = all_securities()
        r.set(TodayKey,result)
    return result



@app.get("/app/test/",summary="测试接口")
async def test():
    '''
    测试接口
    '''
    a = 5
    b = 10
    result = a+b
    return "yes"

if __name__ == "__main__": 

    uvicorn.run(app="Utils:app",host="192.168.1.57",port=8000,reload=True)
    # uvicorn.run(app="Utils:app",host="localhost",port=8000,reload=True)