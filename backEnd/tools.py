'''
Author: zivgogogo jhincs@csu.edu.cn
Date: 2023-06-29 17:07:06
LastEditors: zivgogogo jhincs@csu.edu.cn
LastEditTime: 2023-07-03 21:12:53
FilePath: \frank_branch\tools.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import time
from functools import wraps
import datetime

def calculate_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录函数开始时间
        result = func(*args, **kwargs)  # 执行被装饰的函数
        end_time = time.time()  # 记录函数结束时间
        execution_time = end_time - start_time  # 计算时间差
        print(f"{func.__name__} 的运行时间为 {execution_time} 秒")
        return result
    return wrapper


def execute_at_time(hour1, minute1,hour2,minute2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = datetime.datetime.now()
            start_time = current_time.replace(hour=hour1, minute=minute1, second=0, microsecond=0)
            end_time = current_time.replace(hour=hour2, minute=minute2, second=0, microsecond=0)
            if start_time<=current_time<=end_time :
                cal_time = datetime.datetime.now()
                result=func(*args,**kwargs)
                print('任务耗时',datetime.datetime.now()-cal_time)
                return result
            else:
                print('未在执行时间,不执行')
        return wrapper

    return decorator
