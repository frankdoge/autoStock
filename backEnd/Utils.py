import easyquotation
import easytrader
import datetime
import pandas as pd
import tushare as ts
import redis
import psycopg2
from fastapi import FastAPI,Header,Body,Form,Request
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
# 创建一个scheduler实例
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

# 调用easytrader下单程序
bigdata1 = easytrader.use('universal_client')#定义使用universal_client

#使用sina开源接口来获取实时数据
quotation = easyquotation.use('sina')

# 使用fastapi作为web服务框架
app = FastAPI(title='股票自动交易系统',description='基于vue+fastapi的股票策略选定观望自行交易系统的接口文档')

pro = ts.pro_api('ebd374898a15c69a938b0b6c2de6beb2854b694a4bbe6c731640bfc4')

# 连接postgresql数据库
conn = psycopg2.connect(database='stock', user='postgres',password='csubigdata',host='localhost',port='5432')
cur = conn.cursor()

# 在本地连接redis
pool = redis.ConnectionPool(host='127.0.0.1', port=6379)
r = redis.Redis(connection_pool=pool, decode_responses=True)

#设置一个全局变量来控制股票订单
update_dict = {}


# 将交易日历设置为全局变量，trade_cal[0]表示当日，trade_cal[1]表示前一日
current = datetime.datetime.now()
before = current + datetime.timedelta(days=-90)
current = current.strftime("%Y%m%d")
before = before.strftime("%Y%m%d")
trade_cal = pro.trade_cal(
    start_date= before, end_date=current, is_open='1'
)['cal_date'].tolist()

# 启动easytrader从而可以访问数据
def start():
    try:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
    except:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
    return "success"

# 从tushare转换为数据库格式
def my_normalize_code(code):
    dic_code = {'sz':'XSHE','sh':'XSHG'}
    code_pre = code[:2]
    new_code = code[2:] + '.' + dic_code[code_pre]
    return new_code
# 获得所有股票代码
def get_all_stock_code():
    quotation = easyquotation.use('sina')
    data = quotation.market_snapshot(prefix=True)
    code_list = list(data.keys())
    new_code_list = [my_normalize_code(code) for code in code_list]
    return new_code_list
# 更新股票交易日历
def updateTradeCal():
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
    try:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
    except:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
    return "success"

#将股票代码转换为easyquotation开源接口格式
def denormalize_code(code:str):
    dic_code = {'XSHE':'sz','XSHG':'sh'}
    code_orig = code[7:]
    code_new = dic_code[code_orig] + code[:6]
    return code_new

def all_securities():
    # 获取当前时间和半个月前时间来获取上一个交易日
    day_before = trade_cal[1]
    sql1 = 'SELECT name,close,code FROM daily_stock where date = \'%s\'' %(day_before)
    cur.execute(sql1)
    data = cur.fetchall()
    df = pd.DataFrame(data,columns=['name','close','code'])
    return df.to_json(orient="records")


def isOpen():
    # 检查当前是否为开盘时间
    current_time = datetime.datetime.now()
    current_day = current_time.date()
    print(f"The current time is {current_time}")
    start_time = datetime.datetime(current_day.year,current_day.month,current_day.day,9,30,0)
    end_time1 = start_time + datetime.timedelta(minutes=120)
    end_time2 = start_time + datetime.timedelta(minutes=330)
    if current_time<start_time or current_time>end_time2 or (current_time>end_time1 and current_time<(end_time1+datetime.timedelta(minutes=60))):
        print("当前正休市，任务不执行")
        return False
    return True

# 根据股票代码和id来检查买入订单从而更新数据库
def buyUpdate(code,id):
    positionInfo = bigdata1.position
    for element in positionInfo:
        # 每个element是一个字典
        if element['当日买入'] != 0:
            # 检查发现股票代码对应上
            if element['证券代码'] == code[:6]:
                price = element['成本价1']
                amount = element['当日买入']/100
                state = 1
                updated_time = str(datetime.datetime.now().date())
                sqlUpdate = 'update stockorder set state =%d,updated_time = %s,amount = %d,buy_price = %f where id = %d' %(state,updated_time,amount,price,id)
                print(sqlUpdate)
                cur.execute(sqlUpdate)
                return

# 根据股票代码和id来检查买入订单从而更新数据库
def sellUpdate(code,id,price):
    positionInfo = bigdata1.position
    for element in positionInfo:
        # 每个element是一个字典
        if element['当日卖出'] != 0:
            # 检查发现股票代码对应上
            if element['证券代码'] == code[:6]:
                amount = element['当日卖出']/100
                state = 2
                updated_time = str(datetime.datetime.now().date())
                sqlUpdate  = 'update stockorder set state =%d,updated_time = \'%s\',amount = %d,sell_price = %f where id = %d' %(state,updated_time,amount,price,id)
                print(sqlUpdate)
                cur.execute(sqlUpdate)
                return

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

