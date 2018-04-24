#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 9:12
# @Author  : liulijun
# @Site    :
# @File    : type3MW.py
# @Software: PyCharm

import logging
from datetime import datetime
import pymysql
import pandas as pd
import os
from apscheduler.schedulers.blocking import BlockingScheduler
os.environ['CLASSPATH'] = "./Lib/my.golden.jar"
from jnius import autoclass
from calFunctionUtils import readTagIndex
logname = "run_halt_time.log"
filehandler = logging.FileHandler(filename=logname, encoding="utf-8")
fmter = logging.Formatter(fmt="%(levelname)s %(threadName)s %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
filehandler.setFormatter(fmter)
loger = logging.getLogger(__name__)
loger.addHandler(filehandler)
loger.setLevel(logging.DEBUG)

class main:
    def __init__(self):
        self.last_cal_run_halt_time = pd.DataFrame()
        self.initialer()
        self.updater()
        finish_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('update finished\'s time:', finish_time)

    def initialer(self): # 初始化，以数据库中最后一条记录为准，对机组的时间戳、运行时间和停机时间进行初始化
        conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind',charset="utf8")
        for wtgs_id in range(30002001, 30002018):
            sqlstr = "SELECT * FROM healthy_state_run_halt_time WHERE realTime=(SELECT MAX(realTime) FROM healthy_state_run_halt_time WHERE wtgsId=\'"+str(wtgs_id)+"\')"
            latest_cal_state = pd.read_sql(sql=sqlstr,con=conn)
            self.last_cal_run_halt_time=pd.concat([self.last_cal_run_halt_time,latest_cal_state])
        conn.close()

    def updater(self):# 刷新

        to_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('update start\'s time:', to_time)
        server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
        server = server_impl("192.168.0.37", 6327, "mywind", "MyData@2018")
        historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
        his = historian_impl(server)
        for wtgs_id in range(30002001,30002018):
            from_time=str(self.last_cal_run_halt_time[self.last_cal_run_halt_time['wtgsId']==wtgs_id]['realTime'].iloc[0])
            run_mode = self.last_cal_run_halt_time[self.last_cal_run_halt_time['wtgsId'] == wtgs_id]['runMode'].iloc[0]
            run_time=self.last_cal_run_halt_time[self.last_cal_run_halt_time['wtgsId']==wtgs_id]['runTime'].iloc[0]
            halt_time=self.last_cal_run_halt_time[self.last_cal_run_halt_time['wtgsId']==wtgs_id]['haltTime'].iloc[0]
            run_mode_id=readTagIndex('giwindturbineoperationmode',wtgs_id)
            latest_run_mode_record=queryDataFromGolden(run_mode_id,from_time,to_time,his)
            if len(latest_run_mode_record)>0:
                latest_run_mode_result=cal_run_halt_time(wtgs_id,from_time,run_mode, run_time, halt_time, latest_run_mode_record)
                self.exporter(wtgs_id,to_time,latest_run_mode_result)
            else:
                loger.warning(str(wtgs_id) + " " + str(from_time)+" " +str(to_time) + " no run mode record!")
        server.close()
        his.close()

    def exporter(self,wtgs_id,to_time,latest_run_mode_result):# 输出机组运行停机数据到数据库
        try:
            conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind',charset="utf8")
            cur=conn.cursor()
            insert_value=''
            for row in range(len(latest_run_mode_result.values)):
                insert_value+='(\''+'\',\''.join(latest_run_mode_result.values[row])+'\'),'
            sqlstr = "INSERT INTO healthy_state_run_halt_time VALUES "+insert_value[0:-1]
            cur.execute(sqlstr)
            conn.commit()
            conn.close()
        except:
            loger.warning("插入数据: " + str(wtgs_id) + " " + str(to_time) + " error!")
        finally:
            pass

def queryDataFromGolden(tag_id,start_time, end_time,his):#查数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    result = his.getIntArchivedValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    run_mode_list={}
    if result.size() > 1:
        for i in range(result.size()):
            r = result.get(i)
            run_mode_list[data_unit.dateToString(r.getDateTime())]=int(r.getValue())
        return run_mode_list
    else:
        return []

def cal_run_halt_time(wtgs_id,from_time,run_mode,run_time,halt_time,latest_run_mode_record):
    latest_run_mode_result={}
    latest_run_mode_result['realTime'] = []
    latest_run_mode_result['wtgsId'] = []
    latest_run_mode_result['runMode'] = []
    latest_run_mode_result['runTime'] = []
    latest_run_mode_result['haltTime'] = []
    this_state_from_time=from_time
    run_state_code =[12,13,14]
    halt_state_code =[i for i in range(0,12)]+[15,30,31,32,33,50,51]
    for newTime,newRunMode in latest_run_mode_record.items():
        if timeDelta(from_time,newTime)>0:
            if run_mode in  halt_state_code and newRunMode in run_state_code:# 停机转启动并网
                halt_time+=timeDelta(this_state_from_time,newTime)
                run_time-=timeDelta(this_state_from_time,newTime)
                if halt_time>=1440:
                    halt_time=1440
                elif halt_time<0 or run_time>360 :
                    halt_time=0
                if run_time<0:
                    run_time=0
                elif run_time>600:
                    run_time=600
            elif run_mode in run_state_code and  newRunMode in halt_state_code:# 并网转停机
                halt_time -= timeDelta(this_state_from_time, newTime)
                run_time += timeDelta(this_state_from_time, newTime)
                if halt_time>=1440:
                    halt_time=1440
                elif halt_time<0 :
                    halt_time=0
                if run_time<0:
                    run_time=0
                elif run_time>600:
                    run_time=600
            elif run_mode in run_state_code and  newRunMode in run_state_code: # 持续并网
                halt_time -= timeDelta(this_state_from_time, newTime)
                run_time += timeDelta(this_state_from_time, newTime)
                if halt_time>=1440:
                    halt_time=1440
                elif run_time>360 or halt_time<0:
                    halt_time=0
                if run_time<0:
                    run_time=0
                elif run_time>600:
                    run_time=600
            elif run_mode in halt_state_code and  newRunMode in halt_state_code:  # 持续停机
                halt_time += timeDelta(this_state_from_time, newTime)
                run_time -= timeDelta(this_state_from_time, newTime)
                if halt_time>=1440:
                    halt_time=1440
                elif halt_time<0:
                    halt_time=0
                if halt_time>30 or run_time<0:
                    run_time=0
                elif run_time>600:
                    run_time=600
            latest_run_mode_result['realTime'].append(newTime)
            latest_run_mode_result['wtgsId'].append(str(wtgs_id))
            latest_run_mode_result['runMode'].append(str(newRunMode))
            latest_run_mode_result['runTime'].append(str(round(run_time,2)))
            latest_run_mode_result['haltTime'].append(str(round(halt_time,2)))
            run_mode = newRunMode
            this_state_from_time = newTime
    latest_run_mode_result=pd.DataFrame.from_dict(latest_run_mode_result)
    latest_run_mode_result=latest_run_mode_result[['realTime','wtgsId','runMode','runTime','haltTime']]
    return latest_run_mode_result

def timeDelta(from_time,to_time):# 时间差
    delta=datetime.strptime(to_time,"%Y-%m-%d %H:%M:%S")-datetime.strptime(from_time,"%Y-%m-%d %H:%M:%S")
    return round(delta.days*24*60+delta.seconds/60,2)

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(main, 'interval', seconds=300, replace_existing=True)
    try:
        scheduler.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')