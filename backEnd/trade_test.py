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
# 调用easytrader下单程序
bigdata1 = easytrader.use('universal_client')#定义使用universal_client
quotation = easyquotation.use('sina')
try:
    bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
except:
    bigdata1.prepare(user='123456789', comm_password='123456',exe_path=r'C:\\ths\\xiadan.exe')
print("init finish")

# today = str(datetime.datetime.now().date())
# print(today)
returnInfo = bigdata1.balance
print(returnInfo)
