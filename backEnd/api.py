from distutils.command import config
import json
from pdb import post_mortem
from flask import Flask,jsonify,request
from jqdatasdk import *
import time
import easytrader 
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import time
# import tushare as ts
# ts.set_token('ebd374898a15c69a938b0b6c2de6beb2854b694a4bbe6c731640bfc4')
# pro = ts.pro_api()
# auth('18974988801','Bigdata12345678')
# auth('15367831365','Wsj5201314')
auth('15707956978','CSUbigdata123')
# import settings
bigdata1 = easytrader.use('universal_client')#定义使用universal_client


app = Flask(__name__)
# Flask 是一个类，我们可以点进去看详细的描述。app是Flask这个类创建出来的对象。
# __name__是获取当前文件的名字，我们可以尝试print(__name__)，就会看见结果是：__main__。
# print(app.config)                               
# 可以通过app.config查看所有参数。
app.config.from_object(config)

def all_security_list() -> json:
    df2 = get_all_securities(['stock'])
    list1 = list(df2.index)
    df1 = get_price(list1,None,'2023-05-20','daily','close',True,'pre',count=1,panel=False,fill_paused=True)
    df1['name'] = df2.loc[list(df1['code'])].display_name.values
    return df1.to_json(orient="records")

def MA(md1:int, md2:int) -> list:
    df1 = get_price(list(get_all_securities(['stock']).index),None,'2023-05-20','daily','close',True,'pre',count=md2,panel=False,fill_paused=True)
    return [df1.iloc[i + 1, 1] for i in range(0, df1.shape[0], md2) if df1.iloc[i : i + md1 - 1, 2].mean() > df1.iloc[i : i + md2 - 1, 2].mean()]
def MADF(md1:int, md2:int) -> json:
    df2 = get_all_securities(['stock'])
    list1 = list(df2.index)
    df1 = get_price(list1,None,'2023-05-20','daily','close',True,'pre',count=md2,panel=False,fill_paused=True)
    list2 = [df1.iloc[i + 1, 1] for i in range(0, df1.shape[0], 10) if df1.iloc[i : i + md1 - 1, 2].mean() > df1.iloc[i : i + md2 - 1, 2].mean()]
    df3 = get_price(list2,None,'2023-05-20','daily','close',True,'pre',count=1,panel=False,fill_paused=True)
    df3['name'] = df2.loc[list(df3['code'])].display_name.values
    return df3.to_json(orient="records")
    

@app.route('/test')# 路由
def MA_data():
    md1 = request.args.get('md1')
    md2 = request.args.get('md2')
    return MADF(int(md1),int(md2))

@app.route('/all_securities')# 路由
def all_securities():
    return all_security_list()

@app.route('/')
def hello_for_use_api():
    return "hello for use api"

def get_stock_data():
    balance = bigdata1.balance
    position =bigdata1.position
    return balance,position
    
@app.route('/start')
def start_process():
    try:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'c:\\ths\\xiadan.exe')
    except:
        bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'c:\\ths\\xiadan.exe')

    return "启动成功"

@app.route('/getdata')
def route_get_data():
    balance,position = get_stock_data()
    return jsonify(balance,position)
@app.route('/mairu')
def mairu():
    code = request.args.get('code')
    price = request.args.get('price')
    amount = request.args.get('amount')
    bigdata1.buy(str(code),price,amount)
    return "下单成功"
@app.route('/maichu')
def maichu():
    code = request.args.get('code')
    price = request.args.get('price')
    amount = request.args.get('amount')
    bigdata1.sell(str(code),price,amount)
    return "下单成功"
@app.route('/jiaogedan')
def jiaogedan():
    return jsonify(bigdata1.today_entrusts)


if __name__ == '__main__':
    #app.run()                                  # 启动flask内部服务器，主机地址和端口号选取默认值
    app.run(port=3300,host="127.0.0.1")        # 启动flask内部服务器，主机地址和端口号可自定义
