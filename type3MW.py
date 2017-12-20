#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 9:12
# @Author  : liulijun
# @Site    : 
# @File    : type3MW.py
# @Software: PyCharm

import logging
import random
import pandas as pd
import pymysql
import time
import calFunctionUtils
import runHaltTime
import os
import numpy
os.environ['CLASSPATH'] = "./Lib/my.golden.jar"
from jnius import autoclass
from datetime import datetime,timedelta
import configparser

logname = "run.log"
filehandler = logging.FileHandler(filename=logname, encoding="utf-8")
fmter = logging.Formatter(fmt="%(levelname)s %(threadName)s %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
filehandler.setFormatter(fmter)
loger = logging.getLogger(__name__)
loger.addHandler(filehandler)
loger.setLevel(logging.DEBUG)

class wtgsCalProcess:
    def __init__(self,wtgs_id,currentTime,his):
        self.current_time=currentTime
        self.from_time = datetime.strptime(self.current_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=-1800) # 整点时间的前后半个小时
        self.from_time=self.from_time.strftime("%Y-%m-%d %H:%M:%S")
        self.to_time = datetime.strptime(self.current_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=+1800) # 整点时间的前后半个小时
        self.to_time=self.to_time.strftime("%Y-%m-%d %H:%M:%S")
        self.wtgs_id=wtgs_id
        self.indicator_dictory=self.indicatorSet()
        [self.healthy_state_index_value,self.healthy_state_component_value]=self.indicatorCalProcess(his)
        self.addRunMode()
        self.addBaseInfo()
        # self.exportIndexResultToRemoteDB()
        # self.exportComponentResultToRemoteDB()

    def indicatorSet(self):
        cfg = configparser.ConfigParser()
        cfg.read('config')
        indicator_list=cfg.sections()
        indicator_dictory={}
        for indicator in indicator_list:
            indicator_dictory[indicator]={}
            for index in cfg.options(indicator):
                indicator_dictory[indicator][index]=cfg.get(indicator, index)
        return  indicator_dictory

    def indicatorCalProcess(self,his):#指标计算过程
        indicator_cal_result={}
        for index, attribute in self.indicator_dictory.items():
            indicator_cal_result[attribute['resultdbfield']]={'flag':0,'value':None,'table':attribute['resulttable']}
        for index,attribute in self.indicator_dictory.items():
            if attribute['dataresource']=='golden' and 'x1_step1_xoffset' not in attribute.keys():# 首先计算最底层指标，非神经网络类
                res=getattr(calFunctionUtils,attribute['calfunc'])(attribute['funcpara'], float(attribute['healthylevel0']),float(attribute['healthylevel100']), self.wtgs_id,self.from_time,self.current_time,self.to_time,his)
                # print(index,res)
                indicator_cal_result[attribute['resultdbfield']]['flag'] = 1
                indicator_cal_result[attribute['resultdbfield']]['value']= res
            elif attribute['dataresource']=='golden' and 'x1_step1_xoffset' in attribute.keys():#计算最底层指标，神经网络类
                # res = getattr(calFunctionUtils, attribute['calfunc'])(attribute,self.wtgs_id,self.current_time)
                res=1
                # print(attribute['indexdsec'],res)
                indicator_cal_result[attribute['resultdbfield']]['flag'] = 1
                indicator_cal_result[attribute['resultdbfield']]['value'] = res
        for index,attribute in self.indicator_dictory.items():
            if attribute['resulttable'] == 'healthy_state_index' and attribute['dataresource']=='afterInitialCal':  # 然后计算中间层指标
                if ',' not in attribute['funcpara']:
                    para_name_list=[attribute['funcpara']]
                else:
                    para_name_list=attribute['funcpara'].split(',') #参数列表
                para_value_dict={name:indicator_cal_result[name] for name in para_name_list} # 参数值列表
                res = getattr(calFunctionUtils,attribute['calfunc'])(self.wtgs_id,self.current_time,para_value_dict)
                # print(attribute['indexdsec'], index,res)
                indicator_cal_result[attribute['resultdbfield']]['flag'] = 1
                indicator_cal_result[attribute['resultdbfield']]['value'] = res
        for index,attribute in self.indicator_dictory.items():
            if attribute['resulttable'] == 'healthy_state_component' and attribute['resultdbfield'] != 'turbine':  # 接着计算部件系统健康度
                if ',' not in attribute['funcpara']:
                    para_name_list=[attribute['funcpara']]
                else:
                    para_name_list=attribute['funcpara'].split(',') #参数列表
                para_value_dict={name:indicator_cal_result[name] for name in para_name_list} # 参数值列表
                res = getattr(calFunctionUtils,attribute['calfunc'])(self.wtgs_id,self.current_time,para_value_dict)
                # print(attribute['indexdsec'], res)
                indicator_cal_result[attribute['resultdbfield']]['flag'] = 1
                indicator_cal_result[attribute['resultdbfield']]['value'] = res
        for index, attribute in self.indicator_dictory.items():
            if attribute['resulttable'] == 'healthy_state_component' and attribute['resultdbfield'] == 'turbine':  # 最后计算整机健康度
                if ',' not in attribute['funcpara']:
                    para_name_list = [attribute['funcpara']]
                else:
                    para_name_list = attribute['funcpara'].split(',')  # 参数列表
                para_value_dict = {name: indicator_cal_result[name] for name in para_name_list}  # 参数值列表
                res = getattr(calFunctionUtils,attribute['calfunc'])(self.wtgs_id, self.current_time, para_value_dict)
                indicator_cal_result[attribute['resultdbfield']]['flag'] = 1
                indicator_cal_result[attribute['resultdbfield']]['value'] = res
        indicator_cal_result['grGearboxBypassPumpPressure']={'flag':1,'value':1.0,'table':'healthy_state_index'} # 标签点没有，所建结果表中有，后面确定没有该标签点，删除
        da=pd.DataFrame.from_dict(indicator_cal_result).T
        healthy_state_index_value=da[da['table']=='healthy_state_index']['value']
        healthy_state_component_value = da[da['table']=='healthy_state_component']['value']
        return healthy_state_index_value,healthy_state_component_value

    def addRunMode(self):#添加运行模式
        # run_mode_id=calFunctionUtils.readTagIndex('giwindturbineoperationmode',self.wtgs_id)
        # run_mode_value=runHaltTime.queryDataFromGolden(run_mode_id,self.from_time,self.to_time)
        self.healthy_state_component_value['runMode'] = 14

    def addBaseInfo(self):#添加基本信息
        self.healthy_state_component_value['realTime'] = self.current_time
        self.healthy_state_component_value['farmCode'] = 30002
        self.healthy_state_component_value['farmName'] ="MySE3.0-121"
        self.healthy_state_component_value['wtgsId'] = self.wtgs_id
        self.healthy_state_component_value['wtgsName'] = str(self.wtgs_id%30002000)+"#"
        self.healthy_state_index_value['realTime'] = self.current_time
        self.healthy_state_index_value['wtgsId'] = self.wtgs_id

    def exportIndexResultToRemoteDB(self):#输出子指标结果到远程数据库
        try:
            if self.healthy_state_component_value['runMode']: # 有运行状态才输出
                insert_tag_name = self.healthy_state_index_value.index.tolist()
                insert_tag_value = self.healthy_state_index_value.values.tolist()
                insert_tag_value_format=[]
                for value in insert_tag_value:
                    if str(value) <= '1' or type(value) == numpy.float:
                        insert_tag_value_format.append(str(value))
                    else:
                        insert_tag_value_format.append(str(value))
                        pass
                insert_tag_name = ",".join(insert_tag_name)
                insert_tag_value = "\'"+"\',\'".join(insert_tag_value_format)+"\'"
                conn=pymysql.connect(host='192.168.0.19',port=3306,user='llj',passwd='llj@2016',db='iot_wind',charset="utf8")
                cur=conn.cursor()
                sqlstr="REPLACE INTO healthy_state_index("+insert_tag_name+") VALUES("+insert_tag_value+")"
                cur.execute(sqlstr)
                conn.commit()
                conn.close()
        except:
            loger.warning("Insert data to index table error!")
        finally:
            pass

    def exportComponentResultToRemoteDB(self):  # 输出综合指标结果到远程数据库
        try:
            if self.healthy_state_component_value['runMode']:  # 有运行状态才输出
                insert_tag_name = self.healthy_state_component_value.index.tolist()
                insert_tag_value = self.healthy_state_component_value.values.tolist()
                insert_tag_value_format = []
                for value in insert_tag_value:
                    if str(value)<='1' or type(value)==numpy.float:
                        insert_tag_value_format.append(str(value))
                    else:
                        insert_tag_value_format.append(str(value))
                        pass
                insert_tag_name = ",".join(insert_tag_name)
                insert_tag_value = "\'" + "\',\'".join(insert_tag_value_format) + "\'"
                conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind',charset="utf8")
                cur = conn.cursor()
                sqlstr = "REPLACE INTO healthy_state_component(" + insert_tag_name + ") VALUES(" + insert_tag_value + ")"
                cur.execute(sqlstr)
                conn.commit()
                conn.close()
        except:
            loger.warning("Insert data to component table error!")
        finally:
            pass

class mainLoopProcess:
    def __init__(self):
        conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind',charset="utf8")
        sqlstr = "SELECT MAX(realTime) FROM healthy_state_component"
        latest_cal_time = pd.read_sql(sql=sqlstr, con=conn)
        conn.close()
        from_time=str(latest_cal_time['MAX(realTime)'].iloc[0]) # 已经计算的最新时间
        # from_time="2017-12-08 00:00:00"
        from_time=datetime.strptime(from_time,"%Y-%m-%d %H:%M:%S")
        currentTime=datetime.now().strftime("%Y-%m-%d %H")+":00:00"#整点计算时间
        currentTime = datetime.strptime(currentTime, "%Y-%m-%d %H:%M:%S")

        while from_time<=currentTime:
            self.currentTime = from_time.strftime("%Y-%m-%d %H:%M:%S")  # 整点计算时间
            print(self.currentTime)
            self.loopWtgs()
            from_time=from_time+timedelta(seconds=3600)
            # break

    def loopWtgs(self):#循环机组
        server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
        server = server_impl("192.168.0.37", 6327, "sa", "golden")
        historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
        his = historian_impl(server)
        for wtgs_id in range(30002001,30002018):
            print(wtgs_id)
            wtgsCalProcess(wtgs_id,self.currentTime,his)
            # break
        server.close()
        his.close()


if __name__=="__main__":
    while True:
        if datetime.now().strftime("%Y-%m-%d %H:%M:%S")[14:16]=='40':
            mainLoopProcess()
        else:
            pass
    # mainLoopProcess()





