#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 17:03
# @Author  : liulijun
# @Site    : 
# @File    : calFunctionUtils.py
# @Software: PyCharm

import logging
import sqlite3
import pymysql
import pandas as pd
import os
from  datetime import datetime,timedelta
import numpy
os.environ['CLASSPATH'] = "./Lib/my.golden.jar"
from jnius import autoclass
logname = "run.log"
filehandler = logging.FileHandler(filename=logname, encoding="utf-8")
fmter = logging.Formatter(fmt="%(levelname)s %(threadName)s %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
filehandler.setFormatter(fmter)
loger = logging.getLogger(__name__)
loger.addHandler(filehandler)
loger.setLevel(logging.DEBUG)

def grPitch1MotorOpetationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶1电机工作时间
    # TODO-TESTED
    res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        return query_last_state(wtgs_id,current_time,'grPitch1MotorOpetationTime','healthy_state_index')
    else:
        return res

def grPitch1BackupPowerCapacity(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶1后备电源容量
    # TODO-TESTED
    res=ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch1BackupPowerCapacity', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch1FollowingDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶1跟随偏差
    # TODO-TESTED-加故障逻辑判断, 不故障的时候才算
    return 1
    # res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    # if res is None:#如果返回值为空
    #     value_last_state = query_last_state(wtgs_id, current_time, 'grPitch1FollowingDifference', 'healthy_state_index')
    #     if value_last_state == 0:
    #         return 1
    #     else:
    #         return value_last_state
    # else:
    #     return res

def grPitch2MotorOpetationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶2电机工作时间
    # TODO-TESTED
    res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch2MotorOpetationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch2BackupPowerCapacity(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶2后备电源容量
    # TODO-TESTED
    res=ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch2BackupPowerCapacity', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch2FollowingDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶2跟随偏差
    # TODO-TESTED
    res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch2FollowingDifference', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch3MotorOpetationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶3电机工作时间
    # TODO-TESTED
    res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch3MotorOpetationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch3BackupPowerCapacity(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶3后备电源容量
    # TODO-TESTED
    res=ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch3BackupPowerCapacity', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch3FollowingDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#桨叶3跟随偏差
    # TODO-TESTED
    res=descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch3FollowingDifference', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grPitch1HealthyLevel(wtgs_id,current_time,para_value_dict):#桨叶1总健康度
    # TODO-TESTED
    flag=0
    value_sum=0
    num=0
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag=1
        else:
            if attr['value']>0:
                value_sum+=1/attr['value']
                num+=1
    if flag==0:
        if value_sum>0:
            return round(num/value_sum,4)
        else:
            return 0
    else:
        loger.warning("桨叶1总健康度 " + str(wtgs_id) + " " + str(current_time)+" is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch1HealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grPitch2HealthyLevel(wtgs_id,current_time,para_value_dict):#桨叶2总健康度
    # TODO-TESTED
    flag = 0
    value_sum = 0
    num=0
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            if attr['value']>0:
                value_sum += 1 / attr['value']
                num+=1
    if flag==0:
        if value_sum>0:
            return round(num/value_sum,4)
        else:
            return 0
    else:
        loger.warning("桨叶2总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch2HealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grPitch3HealthyLevel(wtgs_id,current_time,para_value_dict):#桨叶3总健康度
    # TODO-TESTED
    flag = 0
    value_sum = 0
    num=0
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            if attr['value']>0:
                value_sum += 1 / attr['value']
                num+=1
    if flag==0:
        if value_sum>0:
            return round(num/value_sum,4)
        else:
            return 0
    else:
        loger.warning("桨叶3总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch3HealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grPitch(wtgs_id,current_time,para_value_dict):#桨叶总健康度
    # TODO-TESTED
    flag = 0
    value_list=[]
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append( attr['value'] )
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("变桨系统总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grPitch', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxMainBearingTemperature(attribute,wtgs_id,current_time,his):#齿轮箱主轴承温度
    # TODO-齿轮箱主轴承温度
    if queryRunMode(wtgs_id,current_time)!=14:
        return 1
    else: # 并网的条件下才看健康度
        predict_value_list=[]
        actual_value_list=[]
        for delta in range(-30,31):
            base_time_loop=hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict=weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-一小时前齿轮箱主轴温度：grgearboxmainbearingtemperature
            grGearboxMainbearingTemperature1HoursAgo = getFloatInterpoValuesFromGolden2('grgearboxmainbearingtemperature', wtgs_id,  hisOrFurTime(base_time_loop, -5, -1, 0),  hisOrFurTime(base_time_loop, 0, -1, 0),his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime,halttime]=queryRunHaltTime(wtgs_id,base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均风速：grwindspeed
            grWindSpeedValue_10MIN = getFloatInterpoValuesFromGolden2('grwindspeed', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-1小时功率平均值：grgridactivepower
            grGridActivepower_1hour = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -3600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输出变量-齿轮箱主轴承温度：grgridactivepower
            grGearboxMainbearingTemperature_now = getFloatInterpoValuesFromGolden2('grgearboxmainbearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            actual_value_list.append(grGearboxMainbearingTemperature_now)
            # 输入变量向量
            argv_dict['input_vector']=[grOutdoorTemperatureValue,grAirDensityValue,grGearboxMainbearingTemperature1HoursAgo,grGridActivePowerValue_30MIN,
                                       grGridActivePowerValue_10MIN,runtime,halttime,grGeneratorSpeed1Value_10MIN,grWindSpeedValue_10MIN,grGridActivepower_1hour]
            if None in argv_dict['input_vector'] or grGearboxMainbearingTemperature_now is None or int(grRunMode)!=14: # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict) # 采用神经网络
                predict_value_list.append(ANN.output) # 神经网络预期输出
        if len(predict_value_list)>0:
            healthy_score=ANNLinearDescend(float(attribute['healthylevel0']),float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            out={'predict':predict_value_list,'actual':actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\"+attribute['indexdsec']+".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxMainBearingTemperature','healthy_state_index')
        return healthy_score

def grGearboxDETemperature(attribute,wtgs_id,current_time,his):#齿轮箱轮毂侧轴承温度
    # TODO-齿轮箱轮毂侧轴承温度
    if queryRunMode(wtgs_id,current_time)!=14:
        return 1
    else: # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        # 神经网络参数
        argv_dict = weightBias(attribute)
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0), hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A1口压力：grgearboxoilpressurea1
            grGearboxoilPressureA1Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A2口压力：grgearboxoilpressurea2
            grGearboxoilPressureA2Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN =  getFloatInterpoValuesFromGolden2('grgridactivepower',wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN =  getFloatInterpoValuesFromGolden2('grgridactivepower',wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN =  getFloatInterpoValuesFromGolden2('grgeneratorspeed1',wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均风速：grwindspeed
            grWindSpeedValue_10MIN =  getFloatInterpoValuesFromGolden2('grwindspeed', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均油位：grgearboxoillevel
            grGearboxOilLevel_10MIN =  getFloatInterpoValuesFromGolden2('grgearboxoillevel',wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 实际输出变量-齿轮箱前轴承温度：grgearboxhubsidebearingtemperature
            grGearboxHubsideBearingTemperature = getFloatInterpoValuesFromGolden2('grgearboxhubsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            actual_value_list.append(grGearboxHubsideBearingTemperature)
            # 输入变量向量
            argv_dict['input_vector']=[grGearboxoilTempertureValue,grGearboxoilPressureA1Value,grGearboxoilPressureA2Value,grOutdoorTemperatureValue,grAirDensityValue,grGridActivePowerValue_30MIN,grGridActivePowerValue_10MIN,runtime,halttime,grGeneratorSpeed1Value_10MIN, grWindSpeedValue_10MIN,grGearboxOilLevel_10MIN]
            if None in argv_dict['input_vector'] or grGearboxHubsideBearingTemperature is None or int(grRunMode)!=14: # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']), abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxDETemperature','healthy_state_index')
        return healthy_score

def grGearboxNDETemperature(attribute,wtgs_id,current_time,his):#齿轮箱发电机侧轴承温度
    #TODO-齿轮箱发电机侧轴承温度
    if queryRunMode(wtgs_id,current_time)!=14:
        return 1
    else: # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        # 神经网络参数
        argv_dict = weightBias(attribute)
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A1口压力：grgearboxoilpressurea1
            grGearboxoilPressureA1Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A2口压力：grgearboxoilpressurea2
            grGearboxoilPressureA2Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A3口压力：grgearboxoilpressurea3
            grGearboxoilPressureA3Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea3', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-A4口压力：grgearboxoilpressurea4
            grGearboxoilPressureA4Value = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea4', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,hisOrFurTime(base_time_loop, -1800, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -300, 0, 0), hisOrFurTime(base_time_loop, 300, 0, 0),his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -300, 0, 0), hisOrFurTime(base_time_loop, 300, 0, 0),his)
            # 输入变量-10分钟平均风速：grwindspeed
            grWindSpeedValue_10MIN = getFloatInterpoValuesFromGolden2('grwindspeed', wtgs_id, hisOrFurTime(base_time_loop, -300, 0, 0), hisOrFurTime(base_time_loop, 300, 0, 0),his)
            # 实际输出变量-齿轮箱后轴承温度：grgearboxgeneratorsidebearingtemperature
            grGearboxGeneratorsideBearingTemperature_Now = getFloatInterpoValuesFromGolden2('grgearboxgeneratorsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            actual_value_list.append(grGearboxGeneratorsideBearingTemperature_Now)
            # 输入变量向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGearboxoilPressureA1Value, grGearboxoilPressureA2Value,
                                         grGearboxoilPressureA3Value, grGearboxoilPressureA4Value,grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN, runtime,
                                         halttime, grGeneratorSpeed1Value_10MIN,grWindSpeedValue_10MIN]
            if None in argv_dict['input_vector'] or grGearboxGeneratorsideBearingTemperature_Now is None or int(grRunMode)!=14: # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']), abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxNDETemperature','healthy_state_index')
        return healthy_score

def grGearbox(wtgs_id,current_time,para_value_dict):#齿轮箱健康度
    # TODO-齿轮箱健康度
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("齿轮箱总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearbox', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGeneratorFloatingVoltage(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机空载电压
    # TODO-TESTED-发电机空载电压
    res=ascend1(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state=query_last_state(wtgs_id,current_time,'grGeneratorFloatingVoltage','healthy_state_index')
        if value_last_state==0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGeneratorWindingTemperature1(attribute,wtgs_id,current_time,his):#发电机绕组1温度
    # TODO-发电机绕组1温度
    if queryRunMode(wtgs_id,current_time)!=14:
        return 1
    else: # 并网的条件下才看健康度
        predict_value_list=[]
        actual_value_list=[]
        for delta in range(-30,31):
            base_time_loop=hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime,halttime]=queryRunHaltTime(wtgs_id,base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -3600, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            # 实际输出变量-发电机绕组1温度：grgeneratorwindingtemperature1
            grGeneratorWindingTemperature1 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),  hisOrFurTime(base_time_loop, 0, 0, 0),his)
            actual_value_list.append(grGeneratorWindingTemperature1)
            #神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grGeneratorWindingTemperature1 is None or int(grRunMode)!=14: # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature1','healthy_state_index')
        return healthy_score

def grGeneratorWindingTemperature2(attribute,wtgs_id,current_time, his):#发电机绕组2温度
    # TODO-发电机绕组2温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,
                                                       hisOrFurTime(base_time_loop, -1, 0, 0),
                                                       hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id,
                                                                           hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                           hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                           his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                      hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                      hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id,
                                                                         hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                         hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id,
                                                                 hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                 hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -1800, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -3600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 实际输出变量-发电机绕组2温度：grgeneratorwindingtemperature2
            grGeneratorWindingTemperature2 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature2', wtgs_id,
                                                                              hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                              hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                              his)
            actual_value_list.append(grGeneratorWindingTemperature2)
            # 神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grGeneratorWindingTemperature2 is None or int(
                    grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature2','healthy_state_index')
        return healthy_score

def grGeneratorWindingTemperature3(attribute,wtgs_id,current_time, his):#发电机绕组3温度
    # TODO-发电机绕组3温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,
                                                       hisOrFurTime(base_time_loop, -1, 0, 0),
                                                       hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id,
                                                                           hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                           hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                           his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                      hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                      hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id,
                                                                         hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                         hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id,
                                                                 hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                 hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -1800, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -3600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 实际输出变量-发电机绕组3温度：grgeneratorwindingtemperature3
            grgeneratorwindingtemperature3 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature3', wtgs_id,
                                                                              hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                              hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                              his)
            actual_value_list.append(grgeneratorwindingtemperature3)
            # 神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grgeneratorwindingtemperature3 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),
                                             abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature3','healthy_state_index')
        return healthy_score

def grGeneratorWindingTemperature4(attribute,wtgs_id,current_time, his):#发电机绕组4温度
    # TODO-发电机绕组4温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,
                                                       hisOrFurTime(base_time_loop, -1, 0, 0),
                                                       hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id,
                                                                           hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                           hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                           his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                      hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                      hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id,
                                                                         hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                         hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id,
                                                                 hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                 hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -1800, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -3600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 实际输出变量-发电机绕组4温度：grgeneratorwindingtemperature4
            grgeneratorwindingtemperature4 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature4', wtgs_id,
                                                                              hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                              hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                              his)
            actual_value_list.append(grgeneratorwindingtemperature4)
            # 神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grgeneratorwindingtemperature4 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),
                                             abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature4','healthy_state_index')
        return healthy_score

def grGeneratorWindingTemperature5(attribute,wtgs_id,current_time, his):#发电机绕组5温度
    # TODO-发电机绕组5温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,
                                                       hisOrFurTime(base_time_loop, -1, 0, 0),
                                                       hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id,
                                                                           hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                           hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                           his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                      hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                      hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id,
                                                                         hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                         hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id,
                                                                 hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                 hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -1800, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -3600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 实际输出变量-发电机绕组5温度：grgeneratorwindingtemperature5
            grgeneratorwindingtemperature5 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature5', wtgs_id,
                                                                              hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                              hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                              his)
            actual_value_list.append(grgeneratorwindingtemperature5)
            # 神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grgeneratorwindingtemperature5 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature5','healthy_state_index')
        return healthy_score

def grGeneratorWindingTemperature6(attribute,wtgs_id,current_time, his):#发电机绕组6温度
    # TODO-发电机绕组6温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id,
                                                       hisOrFurTime(base_time_loop, -1, 0, 0),
                                                       hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id,
                                                                           hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                           hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                           his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                      hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                      hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id,
                                                                         hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                         hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-空气密度：grairdensity
            grAirDensityValue = getFloatInterpoValuesFromGolden2('grairdensity', wtgs_id,
                                                                 hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                 hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -1800, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id,
                                                                            hisOrFurTime(base_time_loop, -3600, 0, 0),
                                                                            hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                            his)
            # 实际输出变量-发电机绕组6温度：grgeneratorwindingtemperature6
            grgeneratorwindingtemperature6 = getFloatInterpoValuesFromGolden2('grgeneratorwindingtemperature6', wtgs_id,
                                                                              hisOrFurTime(base_time_loop, -5, 0, 0),
                                                                              hisOrFurTime(base_time_loop, 0, 0, 0),
                                                                              his)
            actual_value_list.append(grgeneratorwindingtemperature6)
            # 神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGridActivePowerValue, grOutdoorTemperatureValue,
                                         grAirDensityValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grgeneratorwindingtemperature6 is None or int(
                    grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorWindingTemperature6','healthy_state_index')
        return healthy_score

def grGeneratorDEBearingTemperature(attribute,wtgs_id,current_time,his):#发电机齿轮箱侧轴承估计温度
    # TODO 发电机齿轮箱侧轴承估计温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-1小时前的发电机前轴承温度：grgeneratorgearboxsidebearingtemperature
            grGeneratorGearboxsideBearingTemperatureValue_1HOURAgo = getFloatInterpoValuesFromGolden2('grgeneratorgearboxsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, -1, 0),hisOrFurTime(base_time_loop, 5, -1, 0), his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -3600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-发电机前轴承温度：grgearboxgeneratorsidebearingtemperature
            grGeneratorGearboxsideBearingTemperature = getFloatInterpoValuesFromGolden2('grgeneratorgearboxsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGeneratorGearboxsideBearingTemperature)
            # 神经网络输入变量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue,grGeneratorGearboxsideBearingTemperatureValue_1HOURAgo,
                                         grGridActivePowerValue, grOutdoorTemperatureValue, grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN,
                                         runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grGeneratorGearboxsideBearingTemperature is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorDEBearingTemperature','healthy_state_index')
        return healthy_score

def grGeneratorNDEBearingTemperature(attribute,wtgs_id,current_time,his):#发电机机舱侧轴承估计温度
    # TODO-发电机机舱侧轴承估计温度
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-1小时前的发电机前轴承温度：grgeneratorgearboxsidebearingtemperature
            grGeneratorGearboxsideBearingTemperatureValue_1HOURAgo = getFloatInterpoValuesFromGolden2('grgeneratornacellesidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, -1, 0),hisOrFurTime(base_time_loop, 5, -1, 0), his)
            # 输入变量-有功功率：grgridactivepower
            grGridActivePowerValue = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-1小时平均功率：grgridactivepower
            grGridActivePowerValue_1HOUR = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -3600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-发电机后轴承温度：grgearboxgeneratorsidebearingtemperature
            grGeneratorNacellesideBearingTemperature = getFloatInterpoValuesFromGolden2('grgeneratornacellesidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGeneratorNacellesideBearingTemperature)
            # 神经网络输入变量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grGeneratorGearboxsideBearingTemperatureValue_1HOURAgo,
                                         grGridActivePowerValue, grOutdoorTemperatureValue, grGridActivePowerValue_30MIN,
                                         grGridActivePowerValue_10MIN,runtime, halttime, grGeneratorSpeed1Value_10MIN, grGridActivePowerValue_1HOUR]
            if None in argv_dict['input_vector'] or grGeneratorNacellesideBearingTemperature is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            else:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGeneratorNDEBearingTemperature','healthy_state_index')
        return healthy_score

def grGenerator(wtgs_id,current_time,para_value_dict):# 发电机健康度
    # TODO-发电机健康度
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("发电机总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGenerator', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxMainPumpMotorOperationTime1(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵1运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxMainPumpMotorOperationTime1', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res 

def grGearboxMainPumpMotorOperationTime2(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵2运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxMainPumpMotorOperationTime2', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGearboxMotorHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱主泵电机健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("齿轮箱主泵电机健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxMotorHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxOilPressureA2(attribute,wtgs_id,current_time,his):#齿轮箱A2压力
    #   TODO-齿轮箱A2压力
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2("grgearboxoiltemperture", wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱前轴承温度：grgearboxhubsidebearingtemperature
            grgearboxhubsidebearingtemperature = getFloatInterpoValuesFromGolden2('grgearboxhubsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1高速：gbgearboxpumphighspeedon1
            gbGearboxPumpHighSpeedon1 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2高速：gbgearboxpumphighspeedon2
            gbGearboxPumpHighSpeedon2 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1低速：gbgearboxpumplowerspeedon1
            gbGearboxPumpLowerSpeedOn1 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2低速：gbgearboxpumphighspeedon1
            gbGearboxPumpLowerSpeedOn2 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-齿轮箱A2压力：grgearboxoilpressurea2
            grGearboxOilPressureA2 = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGearboxOilPressureA2)
            #神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grgearboxhubsidebearingtemperature,
                                         gbGearboxPumpHighSpeedon1, gbGearboxPumpHighSpeedon2, gbGearboxPumpLowerSpeedOn1,gbGearboxPumpLowerSpeedOn2]
            if None in argv_dict['input_vector'] or grGearboxOilPressureA2 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            elif gbGearboxPumpHighSpeedon1==1 and gbGearboxPumpHighSpeedon2==1:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list)-meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxOilPressureA2','healthy_state_index')
        return healthy_score

def grGearboxOilPressureA3(attribute,wtgs_id,current_time,his):#齿轮箱A3压力
    # TODO-齿轮箱A3压力
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2("grgearboxoiltemperture", wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱前轴承温度：grgearboxhubsidebearingtemperature
            grgearboxhubsidebearingtemperature = getFloatInterpoValuesFromGolden2('grgearboxhubsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1高速：gbgearboxpumphighspeedon1
            gbGearboxPumpHighSpeedon1 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2高速：gbgearboxpumphighspeedon2
            gbGearboxPumpHighSpeedon2 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1低速：gbgearboxpumplowerspeedon1
            gbGearboxPumpLowerSpeedOn1 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2低速：gbgearboxpumphighspeedon1
            gbGearboxPumpLowerSpeedOn2 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-齿轮箱A3压力：grgearboxoilpressurea3
            grGearboxOilPressureA3 = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea3', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGearboxOilPressureA3)
            #神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grgearboxhubsidebearingtemperature,
                                         gbGearboxPumpHighSpeedon1, gbGearboxPumpHighSpeedon2, gbGearboxPumpLowerSpeedOn1,gbGearboxPumpLowerSpeedOn2]
            if None in argv_dict['input_vector'] or grGearboxOilPressureA3 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            elif gbGearboxPumpHighSpeedon1 == 1 and gbGearboxPumpHighSpeedon2 == 1:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxOilPressureA3','healthy_state_index')
        return healthy_score

def grGearboxOilPressureA4(attribute,wtgs_id,current_time,his):#齿轮箱A4压力
    # TODO-齿轮箱A4压力
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxoilTempertureValue = getFloatInterpoValuesFromGolden2("grgearboxoiltemperture", wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱前轴承温度：grgearboxhubsidebearingtemperature
            grgearboxhubsidebearingtemperature = getFloatInterpoValuesFromGolden2('grgearboxhubsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1高速：gbgearboxpumphighspeedon1
            gbGearboxPumpHighSpeedon1 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2高速：gbgearboxpumphighspeedon2
            gbGearboxPumpHighSpeedon2 = getIntInterpoValuesFromGolden3('gbgearboxpumphighspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵1低速：gbgearboxpumplowerspeedon1
            gbGearboxPumpLowerSpeedOn1 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon1', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油泵2低速：gbgearboxpumphighspeedon1
            gbGearboxPumpLowerSpeedOn2 = getIntInterpoValuesFromGolden3('gbgearboxpumplowerspeedon2', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-齿轮箱A4压力：grgearboxoilpressurea4
            grGearboxOilPressureA4 = getFloatInterpoValuesFromGolden2('grgearboxoilpressurea4', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGearboxOilPressureA4)
            #神经网络输入向量
            argv_dict['input_vector'] = [grGearboxoilTempertureValue, grgearboxhubsidebearingtemperature,
                                         gbGearboxPumpHighSpeedon1, gbGearboxPumpHighSpeedon2, gbGearboxPumpLowerSpeedOn1,gbGearboxPumpLowerSpeedOn2]
            if None in argv_dict['input_vector'] or grGearboxOilPressureA4 is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            elif gbGearboxPumpHighSpeedon1 == 1 and gbGearboxPumpHighSpeedon2 == 1:
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxOilPressureA4','healthy_state_index')
        return healthy_score

def grGearboxLubricationPumpHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱润滑泵健康度
    # TODO-齿轮箱润滑泵健康度
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(sum(value_list)/len(value_list), 4)
    else:
        loger.warning("齿轮箱润滑泵健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxLubricationPumpHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxMainPumpFilter1ErrorTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵滤芯1堵塞时温度
    # TODO-TESTED-齿轮箱主泵滤芯1堵塞时温度-强制设为1, 后续加堵塞信号
    # res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    # if res==0 or res is None:
    #     return 1
    # # if res is None:#如果返回值为空
    # #     return query_last_state(wtgs_id,current_time,'grGearboxMainPumpFilter1ErrorTemperature','healthy_state_index')
    # else:
    #     return res
    return 1

def grGearboxMainPumpFilter2ErrorTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵滤芯2堵塞时温度
    # TODO-TESTED-齿轮箱主泵滤芯2堵塞时温度-强制设为1, 后续加堵塞信号
    # res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    # if res==0 or res is None:
    #     return 1
    # # if res is None:#如果返回值为空
    # #     return query_last_state(wtgs_id,current_time,'grGearboxMainPumpFilter2ErrorTemperature','healthy_state_index')
    # else:
    #     return res
    return 1

def grGearboxMainPumpFilterHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱主泵滤芯健康度
    # TODO-TESTED-强制设为1
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        if value_list[0]==0 or value_list[1]==0:
            return value_list[0]+value_list[1]
        elif value_list[0]==0 and value_list[1]==0:
            return 0
        else:
            return round(2/5*(value_list[0]+value_list[1]+1/(1/value_list[0]+1/value_list[1])), 4)
    else:
        loger.warning("齿轮箱主泵滤芯健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxMainPumpFilterHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxOilCoolerMotorOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油冷却器电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxOilCoolerMotorOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGearboxOilTemperature(attribute,wtgs_id,current_time,his):#齿轮箱油温
    # TODO-齿轮箱油温
    if queryRunMode(wtgs_id, current_time) != 14:
        return 1
    else:  # 并网的条件下才看健康度
        predict_value_list = []
        actual_value_list = []
        for delta in range(-30, 31):
            base_time_loop = hisOrFurTime(current_time, delta, 0, 0)
            # 神经网络参数
            argv_dict = weightBias(attribute)
            # 输入变量-运行模式：giwindturbineoperationmode
            grRunMode = getIntInterpoValuesFromGolden3('giwindturbineoperationmode', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-室外温度：groutdoortemperature
            grOutdoorTemperatureValue = getFloatInterpoValuesFromGolden2('groutdoortemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱油冷风扇启动信号：gbgearboxoilfanon
            gbGearboxOilFanonValue = getIntInterpoValuesFromGolden3('gbgearboxoilfanon', wtgs_id, hisOrFurTime(base_time_loop, -1, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱主轴承温度：grgearboxmainbearingtemperature
            grGearboxMainBearingTemperatureValue = getFloatInterpoValuesFromGolden2('grgearboxmainbearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱后轴承温度：grgearboxgeneratorsidebearingtemperature
            grGearboxGeneratorSideBearingTemperatureValue = getFloatInterpoValuesFromGolden2('grgearboxgeneratorsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-齿轮箱前轴承温度：grgearboxhubsidebearingtemperature
            grGearboxHubsideBearingTemperatureValue = getFloatInterpoValuesFromGolden2('grgearboxhubsidebearingtemperature', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-30分钟平均功率：grgridactivepower
            grGridActivePowerValue_30MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -1800, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-10分钟平均功率：grgridactivepower
            grGridActivePowerValue_10MIN = getFloatInterpoValuesFromGolden2('grgridactivepower', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-油冷风扇60分钟的启动次数：gbgearboxoilfanon
            gbGearboxOilFanOn_1hour = gbGearboxOilFanOn('gbgearboxoilfanon', wtgs_id, hisOrFurTime(base_time_loop, -3600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-本次机组运行时间：runtime
            # 输入变量-本次机组停机时间：halttime
            [runtime, halttime] = queryRunHaltTime(wtgs_id, base_time_loop)
            # 输入变量-发电机10分钟平均转速：grgeneratorspeed1
            grGeneratorSpeed1Value_10MIN = getFloatInterpoValuesFromGolden2('grgeneratorspeed1', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-10分钟平均风速：grwindspeed
            grWindSpeedValue_10MIN = getFloatInterpoValuesFromGolden2('grwindspeed', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 输入变量-10分钟平均油位：grgearboxoillevel
            grGearboxOilLevel_10MIN = getFloatInterpoValuesFromGolden2('grgearboxoillevel', wtgs_id, hisOrFurTime(base_time_loop, -600, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            # 实际输出变量-齿轮箱油温：grgearboxoiltemperture
            grGearboxOilTemperture = getFloatInterpoValuesFromGolden2('grgearboxoiltemperture', wtgs_id, hisOrFurTime(base_time_loop, -5, 0, 0),hisOrFurTime(base_time_loop, 0, 0, 0), his)
            actual_value_list.append(grGearboxOilTemperture)
            #神经网络输入变量
            argv_dict['input_vector'] = [grOutdoorTemperatureValue, gbGearboxOilFanonValue, grGearboxMainBearingTemperatureValue,
                                         grGearboxGeneratorSideBearingTemperatureValue,grGearboxHubsideBearingTemperatureValue,
                                         grGridActivePowerValue_30MIN, grGridActivePowerValue_10MIN, gbGearboxOilFanOn_1hour, runtime,
                                         halttime, grGeneratorSpeed1Value_10MIN,grWindSpeedValue_10MIN,grGearboxOilLevel_10MIN]
            if None in argv_dict['input_vector'] or grGearboxOilTemperture is None or int(grRunMode) != 14:  # 空或非并网的时候不运算
                pass
            elif gbGearboxOilFanonValue==1: # 散热风扇开启
                ANN = BP(argv_dict)  # 采用神经网络
                predict_value_list.append(ANN.output)  # 神经网络预期输出
        if len(predict_value_list) > 0:
            healthy_score = ANNLinearDescend(float(attribute['healthylevel0']), float(attribute['healthylevel100']),abs(meanData(predict_value_list) - meanData(actual_value_list)))
            #out = {'predict': predict_value_list, 'actual': actual_value_list}
            #pd.DataFrame.from_dict(out).to_excel("C:\\Users\\llj\\Desktop\\ANN\\" + attribute['indexdsec'] + ".xlsx")
        else:
            healthy_score=query_last_state(wtgs_id,current_time,'grGearboxOilTemperature','healthy_state_index')

        return healthy_score

def grGearboxBypassPumpMotorOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱过滤泵电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxBypassPumpMotorOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGearboxBypassPumpFilterErrorTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱过滤泵滤芯堵塞温度
    # TODO-TESTED-齿轮箱过滤泵滤芯堵塞温度-强制设为1, 后续加堵塞信号
    return 1
    # res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    # if res is None:#如果返回值为空
        # value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxBypassPumpFilterErrorTemperature', 'healthy_state_index')
        # if value_last_state == 0:
        #     return 1
        # else:
        #     return value_last_state
    # else:
    #     return res

def grGearboxRadiatorHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱散热器健康度
    # TODO-齿轮箱散热器健康度
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("齿轮箱散热器健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxRadiatorHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxOilLevel(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油位
    # TODO-TESTED
    res = ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxOilLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGearboxBypassPartHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱过滤系统健康度
    # TODO-齿轮箱过滤系统健康度
    flag = 0
    value_sum = 0
    num=0
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            if attr['value'] > 0:
                value_sum+=1/attr['value']
                num+=1
    if flag==0:
        if value_sum>0:
            return round(num/value_sum, 4)
        else:
            return 0
    else:
        loger.warning("齿轮箱过滤系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxBypassPartHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxLubricatingPartHealthyLevel(wtgs_id,current_time,para_value_dict):#齿轮箱润滑系统润滑部分健康度
    # TODO-齿轮箱润滑系统润滑部分健康度
    flag = 0
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
            break
    if flag==0:
        Motor=min(para_value_dict['grGearboxMainPumpMotorOperationTime1']['value'],para_value_dict['grGearboxMainPumpMotorOperationTime2']['value'])
        Pump=numpy.mean([para_value_dict['grGearboxOilPressureA2']['value'],para_value_dict['grGearboxOilPressureA3']['value'],para_value_dict['grGearboxOilpressureA4']['value']])
        x1 = para_value_dict['grGearboxMainPumpFilter1ErrorTemperature']['value']
        x2 = para_value_dict['grGearboxMainPumpFilter2ErrorTemperature']['value']
        if x1==0 or x2==0:
            Filter= x1+x2
        elif x1==0 and x2==0:
            Filter= 0
        else:
            Filter= round(2/5*(x1+x2+1/(1/x1+1/x2)), 4)
        return round(min(Motor,Pump,Filter,para_value_dict['grGearboxOilLevel']['value'],para_value_dict['grGearboxOilCoolerMotorOperationTime']['value'],para_value_dict['grGearboxOilTemperature']['value']), 4)
    else:
        loger.warning("齿轮箱润滑系统润滑部分健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxLubricatingPartHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGearboxLubrication(wtgs_id,current_time,para_value_dict):#齿轮箱润滑系统健康度
    # TODO-齿轮箱润滑系统健康度
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        x1=value_list[0]
        x2=value_list[1]
        if x1==0 or x2==0:
            return 0
        elif x1==0 or x2==0:
            return x1+x2
        else:
            return round(2/5*(x1+x2+1/(x1+1/x2)), 4)
    else:
        loger.warning("齿轮箱润滑系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGearboxLubrication', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grGeneratorWaterPumpOnTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机水冷泵电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGeneratorWaterPumpOnTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGeneratorWaterPumpPressureDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机水冷泵进出口压力偏差
    # TODO-TESTED-发电机水冷泵进出口压力偏差-加前提条件：并网条件下
    res = ascendMultiVariable(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res==0 or res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGeneratorWaterPumpPressureDifference', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
        if last_state==0:
            return 1
        else:
            return last_state
    else:
        return res

def grGeneratorWaterFanOnTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time_now,his):#发电机水冷风扇电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id, from_time_now, current_time, to_time_now,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grGeneratorWaterFanOnTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grGeneratorWaterTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his): #发电机冷却水估计温度
    # TODO-电机冷却水估计温度-无模型
    return 1

def grGeneratorWaterCooling(wtgs_id,current_time,para_value_dict):#发电机冷却系统健康百分比
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("发电机冷却系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grGeneratorWaterCooling', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grHydraulicPumpMotorOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#液压泵电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grHydraulicPumpMotorOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grHydraulicPumpEstablishPressureTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#液压泵建压时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grHydraulicPumpEstablishPressureTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giYawBrakeOpenTotalValveActiveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#偏航全释放阀动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giYawBrakeOpenTotalValveActiveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giYawBrakeOpenHalfValveActiveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#偏航半释放阀动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giYawBrakeOpenHalfValveActiveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giRotorLockValveActiveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#叶轮锁阀动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giRotorLockValveActiveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giRotorBrakeValveActiveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#叶轮制动阀动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giRotorBrakeValveActiveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grHydraulic(wtgs_id,current_time,para_value_dict):#液压系统健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("液压系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grHydraulic', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grNacelleFanOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#机舱风扇运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grNacelleFanOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grNacelleTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#机舱温度
    #TODO-机舱温度无模型
    return 1

def grNacelleCooling(wtgs_id,current_time,para_value_dict):#机舱散热系统健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("机舱散热系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grNacelleCooling', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grYawMotorOperationTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#偏航电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grYawMotorOperationTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grYawBrakeWearAwayOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#偏航制动器摩擦片运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grYawBrakeWearAwayOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grYaw(wtgs_id,current_time,para_value_dict):#偏航系统健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("偏航系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grYaw', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grConverterWaterPumpMotorOperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器水冷泵电机运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterPumpMotorOperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterWaterPumpInOutPressureDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器水泵进出水压差
    # TODO-变频器水泵进出水压差-无标签点
    return 1

def grConverterWaterCoolingFanMotor1OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器水冷风扇电机1运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterCoolingFanMotor1OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterWaterCoolingFanMotor2OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器水冷风扇电机2运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterCoolingFanMotor2OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterWaterCoolingFanMotor3OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器水冷风扇电机3运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterCoolingFanMotor3OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterWaterCoolingFanMotorHealthyLevel(wtgs_id,current_time,para_value_dict):#变频器水冷风扇电机总健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(sum(value_list)/len(value_list), 4)
    else:
        loger.warning("变频器水冷风扇电机总健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterCoolingFanMotorHealthyLevel', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grConverterWaterTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器估计水温
    # TODO-变频器估计水温-无标签点
    return 1

def grConverterIGBTTemperature(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器IGBT估计水温
    # TODO-变频器IGBT估计水温-无标签点
    # IGBT换热器健康度=(IGBT温度偏差-健康指标100%%)/(健康指标0%%-健康指标100%%)*100％
    # 没有IGBT温度这个标签点
    return 1

def grConverterWaterCooling(wtgs_id,current_time,para_value_dict):#变频器水冷系统健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("变频器水冷系统健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterWaterCooling', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grConverterCabinetFan1OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器控制柜风扇1运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterCabinetFan1OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterCabinetFan2OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器控制柜风扇2运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterCabinetFan2OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterCabinetFan3OperationTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器控制柜风扇3运行时间
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterCabinetFan3OperationTime', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giConverterLineSideMCBActionTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器网侧断路器动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giConverterLineSideMCBActionTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterGeneratorSideMCBActionTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#变频器机侧断路器动作次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterGeneratorSideMCBActionTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grConverterCabinet(wtgs_id,current_time,para_value_dict):#变频器本体健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("变频器本体健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grConverterCabinet', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def grBladePowerDifference(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#叶片功率偏差
    # TODO-叶片功率偏差-模型不成熟
    return 1

def grBlade(wtgs_id,current_time,para_value_dict):#叶片健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("叶片健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grBlade', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def giGearboxMainPumpOnTimes1(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵启动次数1
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxMainPumpOnTimes1', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxMainPumpOnTimes2(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱主泵启动次数2
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxMainPumpOnTimes2', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxBypassPunpOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱旁通过滤泵启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxBypassPunpOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxOilHeaterOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油加热器启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxOilHeaterOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxOilFanOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油风扇启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxOilFanOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxOilCoolerBypassValveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油冷却滤油阀启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxOilCoolerBypassValveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGearboxOilVolumeValveTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#齿轮箱油量阀启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGearboxOilVolumeValveTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGeneratorWaterPumpOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机水冷泵启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGeneratorWaterPumpOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGeneratorWaterFanOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机水冷风扇启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGeneratorWaterFanOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giGeneratorWaterHeaterOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#发电机水加热器启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giGeneratorWaterHeaterOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giHydraulicPumpOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#液压泵启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giHydraulicPumpOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giYawTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#偏航次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giYawTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def giNacelleFanOnTimes(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#机舱风扇启动次数
    # TODO-TESTED
    res = descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res is None:#如果返回值为空
        value_last_state = query_last_state(wtgs_id, current_time, 'giNacelleFanOnTimes', 'healthy_state_index')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state
    else:
        return res

def grNacelleCabinetUPSBatteryWorkTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#机舱柜UPS电池工作时间
    # TODO-TESTED-存储值为0或空时强制设为1
    res = ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res==0 or res is None:
        return 1
    # if res is None:#如果返回值为空
        # value_last_state = query_last_state(wtgs_id, current_time, 'grNacelleCabinetUPSBatteryWorkTime', 'healthy_state_index')
        # if value_last_state == 0:
        #     return 1
        # else:
        #     return value_last_state
    else:
        return res

def grTowerCabinetUPSBatteryWorkTime(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his):#塔基柜UPS电池工作时间
    # TODO-TESTED-存储值为0或空时强制设为1
    res = ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time,his)
    if res==0 or res is None:
        return 1
    # if res is None:#如果返回值为空
        # value_last_state = query_last_state(wtgs_id, current_time, 'grTowerCabinetUPSBatteryWorkTime', 'healthy_state_index')
        # if value_last_state == 0:
        #     return 1
        # else:
        #     return value_last_state
    else:
        return res

def grControlCabinet(wtgs_id,current_time,para_value_dict):#控制柜健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("控制柜健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'grControlCabinet', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state

def turbine(wtgs_id,current_time,para_value_dict):#整机健康度
    # TODO-TESTED
    flag = 0
    value_list = []
    for para,attr in para_value_dict.items():
        if attr['value'] is None:
            flag = 1
        else:
            value_list.append(attr['value'])
    if flag==0:
        return round(min(value_list), 4)
    else:
        loger.warning("整机健康度 " + str(wtgs_id) + " " + str(current_time) + " is empty!")
        value_last_state = query_last_state(wtgs_id, current_time, 'turbine', 'healthy_state_component')
        if value_last_state == 0:
            return 1
        else:
            return value_last_state


def ascend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time_now,his):#线性增加型
    if fromValue > toValue:
        fromValue, toValue = toValue, fromValue
    tag_golden_index = readTagIndex(index, wtgs_id)  # 查询标签点在golden中的id
    try:
        latest_real_data = getFloatInterpoValuesFromGolden(tag_golden_index, from_time_now, to_time_now,his)
        latest_real_data=removeAbnormalDatas(latest_real_data)
        degreelist=[]
        if latest_real_data:
            for realdata in latest_real_data:
                if realdata>toValue:
                    degreelist.append(1)
                elif realdata<=toValue and realdata>fromValue:
                    degreelist.append(round((realdata-fromValue)/(toValue-fromValue),4))
                else:
                    degreelist.append(0)
            return sum(degreelist)/len(degreelist)
        else:# 当前时间无存储值
            loger.warning(str(index) + ' ' + str(wtgs_id) + ' ' + current_time + " is empty!")
            return None
    except:
        loger.debug(index +' '+current_time+ " query data error!")
    finally:
        pass

def ascend1(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time_now,his):#线性增加型-空载电压适用：空载电压为0时默认健康度为1
    if fromValue > toValue:
        fromValue, toValue = toValue, fromValue
    tag_golden_index = readTagIndex(index, wtgs_id)  # 查询标签点在golden中的id
    try:
        latest_real_data = getFloatInterpoValuesFromGolden(tag_golden_index, from_time_now, to_time_now,his)
        latest_real_data=removeAbnormalDatas(latest_real_data)
        degreelist=[]
        if latest_real_data:
            for realdata in latest_real_data:
                if realdata>toValue or realdata==0:
                    degreelist.append(1)
                elif realdata<=toValue and realdata>fromValue:
                    degreelist.append(round((realdata-fromValue)/(toValue-fromValue),4))
                else:
                    degreelist.append(0)
            return sum(degreelist)/len(degreelist)
        else:# 当前时间无存储值
            loger.warning(str(index) + ' ' + str(wtgs_id) + ' ' + current_time + " is empty!")
            return None
    except:
        loger.debug(index +' '+current_time+ " query data error!")
    finally:
        pass

def descend(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time_now,his):#线性减小型\
    if fromValue > toValue:
        fromValue, toValue = toValue, fromValue
    tag_golden_index = readTagIndex(index, wtgs_id)  # 查询标签点在golden中的id
    try:
        latest_real_data = getFloatInterpoValuesFromGolden(tag_golden_index, from_time_now, to_time_now,his)
        latest_real_data=removeAbnormalDatas(latest_real_data)
        degreelist=[]
        if latest_real_data:
            for realdata in latest_real_data:
                if realdata>toValue:
                    degreelist.append(0)
                elif realdata<=toValue and realdata>fromValue:
                    degreelist.append(round((toValue-realdata)/(toValue-fromValue),4))
                else:
                    degreelist.append(1)
            return sum(degreelist)/len(degreelist)
        else:# 当前时间无存储值
            loger.warning(str(index) + ' ' + str(wtgs_id) + ' ' + current_time + " is empty!")
            return None
    except:
        loger.debug(index +' '+current_time+ " query data error!")
    finally:
        pass

def ascendMultiVariable(index, fromValue, toValue, wtgs_id,from_time_now,current_time,to_time_now,his):#多标签点线性增加型
    index_list=index.split(',')
    value_dict={}
    if fromValue>toValue:
        fromValue,toValue=toValue,fromValue
    for index in index_list:
        tag_golden_index = readTagIndex(index, wtgs_id)  # 查询标签点在golden中的id
        if index == 'giwindturbineoperationmode' or index=='gbgeneratorwaterpumpon':
            tag_real_data = getIntInterpoValuesFromGolden(tag_golden_index, from_time_now, to_time_now,his)
        else:
            tag_real_data = getFloatInterpoValuesFromGolden(tag_golden_index, from_time_now, to_time_now, his)
        value_dict[index]=tag_real_data
    value_dict=pd.DataFrame.from_dict(value_dict)
    value_dict=value_dict[(value_dict['giwindturbineoperationmode']==14.0) & (value_dict['gbgeneratorwaterpumpon']==1.0)] # 机组并网且发电机水泵启动信号
    degreeList=[]
    if len(value_dict)>0:
        for row in range(len(value_dict)):
            diff = abs(value_dict['grgeneratorwaterpressurein'].iloc[row] - value_dict['grgeneratorwaterpressureout'].iloc[row])  # 两者差值的绝对值
            if diff > toValue:
                degreeList.append(1)
            elif diff <= toValue and diff > fromValue:
                degreeList.append(round((diff - fromValue) / (toValue - fromValue), 4))
            else:
                degreeList.append(0)
        return sum(degreeList)/len(degreeList)
    else:
        return None

def readTagIndex(tag_EN,wtgs_id):#查询标签点在庚顿数据库中的索引
    conn=sqlite3.connect('./config.db')
    sqlstr="SELECT tag_index_golden FROM tag_golden_index WHERE wtgs_id=\'"+str(wtgs_id)+"\' AND tag_EN=\'"+str(tag_EN)+"\'"
    resid = pd.read_sql(sqlstr, con=conn)
    conn.close()
    if len(resid)>0:
        return(int(resid['tag_index_golden'].iloc[0]))
    else:
        return []

def getFloatInterpoValuesFromGolden(tag_id, start_time, end_time, his):#查浮点型插值数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    result = his.getFloatInterpoValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 0:
        values = []
        for i in range(result.size()):
            r = result.get(i)
            values.append(r.getValue())
        return values
    else:
        return None

def getFloatInterpoValuesFromGolden2(tag_EN,wtgs_id, start_time, end_time, his):#查浮点型插值数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    tag_id=readTagIndex(tag_EN,wtgs_id)
    result = his.getFloatInterpoValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 0:
        values = []
        for i in range(result.size()):
            r = result.get(i)
            values.append(r.getValue())
        return sum(removeAbnormalDatas(values))/len(removeAbnormalDatas(values))
    else:
        return None

def getIntInterpoValuesFromGolden(tag_id, start_time, end_time, his):#查整型插值数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    result = his.getIntInterpoValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 0:
        values = []
        for i in range(result.size()):
            r = result.get(i)
            values.append(int(r.getValue()))
        return values
    else:
        return None

def getIntInterpoValuesFromGolden2(tag_EN,wtgs_id, start_time, end_time, his):#查整型插值数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    tag_id = readTagIndex(tag_EN, wtgs_id)
    result = his.getIntInterpoValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 0:
        values = []
        for i in range(result.size()):
            r = result.get(i)
            values.append(int(r.getValue()))
        return sum(removeAbnormalDatas(values))/len(removeAbnormalDatas(values))
    else:
        return None

def getIntInterpoValuesFromGolden3(tag_EN,wtgs_id, start_time, end_time, his):#查整型插值数据
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    tag_id = readTagIndex(tag_EN, wtgs_id)
    result = his.getIntInterpoValues(tag_id, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 0:
        values = []
        for i in range(result.size()):
            r = result.get(i)
            values.append(int(r.getValue()))
        return values[-1]
    else:
        return None

def gbGearboxOilFanOn(tag_EN,wtgs_id,start_time, end_time,his):#查数据
    tag_golden_index = readTagIndex(tag_EN, wtgs_id)  # 查询标签点在golden中的id
    data_unit = autoclass('com.rtdb.api.util.DateUtil')
    count = pd.date_range(start=start_time, end=end_time, freq='S').size
    result = his.getIntInterpoValues(tag_golden_index, count, data_unit.stringToDate(start_time),data_unit.stringToDate(end_time))
    if result.size() > 1:
        values = []
        turn_on_times=0
        for i in range(result.size()):
            r = result.get(i)
            values.append(r.getValue())
        for i in range(1,len(values)):
            if values[i]==1 and values[i-1]==0:
                turn_on_times+=1
        return turn_on_times
    else:
        return None

class BP:
    def __init__(self,argv_dict):
        self.input_vector=numpy.array(argv_dict['input_vector']) #输入向量
        self.x1_step1_xoffset=numpy.array(argv_dict['x1_step1_xoffset']) # 归一化参数
        self.x1_step1_gain=numpy.array(argv_dict['x1_step1_gain']) # 归一化参数
        self.x1_step1_ymin=argv_dict['x1_step1_ymin'] # 归一化参数
        self.y1_step1_ymin=argv_dict['y1_step1_ymin'] # 逆归一化参数
        self.y1_step1_gain=argv_dict['y1_step1_gain'] # 逆归一化参数
        self.y1_step1_xoffset=argv_dict['y1_step1_xoffset'] # 逆归一化参数
        self.weight = argv_dict['weight']  # 权重字典, 各层的维数不一致
        self.bias = argv_dict['bias']  # 偏执量字典, 各层的维数不一致
        self.input_vector=self.normalized()
        self.output=self.networks()
        self.output = self.normalizedInverse()

    def networks(self): # 网络运算, 获得输出
        local_layer_out = numpy.mat(self.input_vector).T
        for layer in range(len(self.weight)-1): # 隐含层
            local_weight=numpy.mat(self.weight[str(layer)])
            local_layer_out=local_weight*local_layer_out+numpy.mat(self.bias[str(layer)]).T # wx+b mat:n*1
            local_layer_out=numpy.mat([float(numpy.exp(x[0])-numpy.exp(-x[0]))/float(numpy.exp(x[0])+numpy.exp(-x[0])) for x in local_layer_out.tolist()]).T # sigmoid激活函数
        wx=numpy.mat(self.weight[list(self.weight.keys())[-1]])*local_layer_out # 输出层
        return wx.tolist()[0][0]+self.bias[list(self.bias.keys())[-1]]

    def normalized(self): #  输入向量归一化
        return (self.input_vector-self.x1_step1_xoffset)*self.x1_step1_gain+self.x1_step1_ymin

    def normalizedInverse(self): # 输出向量逆归一化
        return (self.output- self.y1_step1_ymin)/self.y1_step1_gain+ self.y1_step1_xoffset

def query_last_state(wtgs_id,current_time,tag,table):
    one_hour_before=datetime.strptime(current_time,"%Y-%m-%d %H:%M:%S")+timedelta(seconds=-3600)
    one_hour_before=one_hour_before.strftime("%Y-%m-%d %H:%M:%S")
    conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind', charset="utf8")
    cur = conn.cursor()
    sqlstr = "SELECT "+tag+" FROM "+table+" WHERE wtgsId=\'" + str(wtgs_id) + "\' AND realTime=\'"+one_hour_before+"\'"
    # print(sqlstr)
    cur.execute(sqlstr)
    one_hour_before_value = cur.fetchall()
    # print(tag,wtgs_id,one_hour_before_value)
    if len(one_hour_before_value) == 0:
        return 1
    else:
        return (float(one_hour_before_value[0][0]))

def quartile(data):
    # 计算箱线图上下限
    QUARTILE=3
    if len(data)>=3:
        data=sorted(data)
        #Q1
        pvalue=(len(data) + 1) * 0.25
        intvalue=int(pvalue)
        floatvalue = pvalue - intvalue
        if floatvalue==0:
            q1 = data[intvalue-1]
        else:
            q1 = data[intvalue - 1] * floatvalue + data[intvalue] * (1 - floatvalue)
        #Q3
        pvalue = (len(data) + 1) * 0.75
        intvalue = int(pvalue)
        floatvalue = pvalue - intvalue
        if floatvalue==0:
            q3 = data[intvalue - 1]
        else:
            q3 = data[intvalue - 1] * floatvalue + data[intvalue] * (1 - floatvalue)
        max_value = q3 + (q3 - q1) * QUARTILE  # upper
        min_value = q1 - (q3 - q1) * QUARTILE  # upper
        return min_value,max_value

def queryRunMode(wtgs_id,current_time): # 查询最新时间current_time机组的运行模式
    conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind', charset="utf8")
    sqlstr = "SELECT runMode FROM healthy_state_run_halt_time WHERE wtgsId=\'" + str(wtgs_id)+"\' AND realTime=(SELECT MAX(realTime) FROM healthy_state_run_halt_time WHERE wtgsId=\'" + str(wtgs_id) + "\' AND realTime<=\'"+current_time+"\')"
    latest_cal_state = pd.read_sql(sql=sqlstr, con=conn)
    return int(latest_cal_state['runMode'].iloc[0])

def queryRunHaltTime(wtgs_id,current_time):
    # 查询到当前计算时刻机组的运行时间和停机时间
    conn = pymysql.connect(host='192.168.0.19', port=3306, user='llj', passwd='llj@2016', db='iot_wind', charset="utf8")
    sqlstr = "SELECT * FROM healthy_state_run_halt_time WHERE wtgsId=\'" + str(wtgs_id)+"\' AND realTime=(SELECT MAX(realTime) FROM healthy_state_run_halt_time WHERE wtgsId=\'" + str(wtgs_id) + "\' AND realTime<=\'"+current_time+"\')"
    latest_cal_state = pd.read_sql(sql=sqlstr, con=conn)
    conn.close()
    start_time=datetime.strptime(str(latest_cal_state['realTime'].iloc[0]),"%Y-%m-%d %H:%M:%S")
    end_time=datetime.strptime(current_time,"%Y-%m-%d %H:%M:%S")
    if int(latest_cal_state['runMode'].iloc[0]) in [12,13,14]:
        run_time=float(latest_cal_state['runTime'].iloc[0])+(end_time-start_time).seconds/60+(end_time-start_time).days*24*60
        if run_time>600:
            run_time=600
        halt_time=float(latest_cal_state['haltTime'].iloc[0])-(end_time-start_time).seconds/60-(end_time-start_time).days*24*60
        if halt_time<0:
            halt_time=0
    else:
        run_time = float(latest_cal_state['runTime'].iloc[0]) - (end_time - start_time).seconds / 60 - (end_time - start_time).days * 24 * 60
        if run_time < 0:
            run_time = 0
        halt_time = float(latest_cal_state['haltTime'].iloc[0]) + (end_time - start_time).seconds / 60 + (end_time - start_time).days * 24 * 60
        if halt_time > 1440:
            halt_time = 1440
    return run_time,halt_time

def removeAbnormalDatas(data):
    # 求均值
    if None in data:
        data=data.remove(None) # 去除None值
    new_data=[]
    if len(data) >= 3:
        [min_value,max_value]=quartile(data)
        for item in data:
            if item<min_value or item>max_value:
                continue
            else:
                new_data.append(item)
    else:
        new_data=data
    return new_data

def meanData(data):
    normal_data=removeAbnormalDatas(data)
    return sum(normal_data)/len(normal_data)

def hisOrFurTime(base_time, seconds_delta, hours_delta, days_delta):
    # 时间范围
    hisOrFurTime = datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=seconds_delta) + timedelta(hours=hours_delta)+timedelta(days=days_delta)
    hisOrFurTime = hisOrFurTime.strftime("%Y-%m-%d %H:%M:%S")
    return hisOrFurTime

def weightBias(attribute):
    # 从配置表中取网络参数构造网络参数字典
    argv_dict={}
    argv_dict['weight'] = {}
    argv_dict['bias'] = {}
    argv_dict['x1_step1_xoffset'] = [float(item) for item in attribute['x1_step1_xoffset'].split(',')]
    argv_dict['x1_step1_gain'] = [float(item) for item in attribute['x1_step1_gain'].split(',')]
    argv_dict['x1_step1_ymin'] = float(attribute['x1_step1_ymin'])
    argv_dict['y1_step1_ymin'] = float(attribute['y1_step1_ymin'])
    argv_dict['y1_step1_gain'] = float(attribute['y1_step1_gain'])
    argv_dict['y1_step1_xoffset'] = float(attribute['y1_step1_xoffset'])
    for layer in range(int(attribute['layers'])-1):# 隐藏层
        weigth_str_list = [jtem.split(',') for jtem in [item for item in attribute['w'+str(layer+1)].split(';\n')]]
        for i in range(len(weigth_str_list)):
            for j in range(len(weigth_str_list[i])):
                weigth_str_list[i][j] = float(weigth_str_list[i][j])
        argv_dict['weight'][str(layer)] = weigth_str_list
        bias_str_list = [float(jtem) for jtem in [item for item in attribute['b'+str(layer+1)].split(',')]]
        argv_dict['bias'][str(layer)] = bias_str_list
    # 输出层
    weigth_str_list = [float(jtem) for jtem in [item for item in attribute['w'+str(int(attribute['layers']))].split(',')]]
    argv_dict['weight'][str(int(attribute['layers'])-1)] = weigth_str_list
    argv_dict['bias'][str(int(attribute['layers'])-1)] = float(attribute['b'+str(int(attribute['layers']))])
    return argv_dict

def ANNLinearDescend(fromValue,toValue,xValue):
    if fromValue>toValue:
        fromValue,toValue=toValue,fromValue
    if xValue<=fromValue:
        return 1
    elif xValue>=toValue:
        return 0
    else:
        return (toValue-xValue)/(toValue-fromValue)

def A234(fromValue,toValue,predict_value,actual_value):
    print(predict_value,actual_value)
    if actual_value*fromValue>toValue:
        if abs(predict_value-actual_value)<=toValue:
            return 1
        elif abs(predict_value-actual_value)>actual_value*fromValue:
            return 0
        else:
            return (actual_value*fromValue-abs(predict_value-actual_value))/(actual_value*fromValue-toValue)
    else:
        print('aa')
        if abs(predict_value-actual_value)<=toValue:
            return 1
        else:
            return 0


if __name__=="__main__":
    server_impl = autoclass('com.rtdb.service.impl.ServerImpl')
    server = server_impl("192.168.0.37", 6327, "sa", "golden")
    historian_impl = autoclass('com.rtdb.service.impl.HistorianImpl')
    his = historian_impl(server)
    tag='grPitch1MotorOpetationTime'
    id=readTagIndex(tag.lower(),30002001)
    res1=getFloatInterpoValuesFromGolden(631895, '2017-12-19 00:30:00', '2017-12-19 01:30:00',his)
    res={}
    res[tag]=res1
    res=pd.DataFrame.from_dict(res)
    res.to_excel("C:/Users/llj/Desktop/data/存储值/"+tag+".xlsx",index=False)



