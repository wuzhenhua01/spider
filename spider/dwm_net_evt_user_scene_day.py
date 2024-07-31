# coding=utf-8
from pyspark import SparkConf, SparkContext
import re, time, datetime
import sys, getopt, math
from pyspark.sql import SparkSession, HiveContext


##获取景区基础数据
def getSrcpath(_dealDate):
    srcPath = 'hdfs://hucang/user/hive/warehouse/lbs.db/dwm_net_evt_user_scene_hour'
    return srcPath + '/day_id=' + _dealDate + '*/area_type=area/*'


def getAreaDestPath(_dealDate):
    _destPath = 'hdfs://hucang/user/hive/warehouse/lbs.db/dwm_net_evt_user_scene_day'
    return _destPath + '/day_id=' + _dealDate


# 获取工作地、居住地目录文件
def getUserResPath(_dealDate):
    _dateDay = datetime.datetime.strptime(_dealDate, "%Y%m%d")
    _dateSub = _dateDay - datetime.timedelta(days=45)
    _dateMonth = _dateSub.strftime("%Y%m")
    _userResiPath = '/user/hive/warehouse/lbs.db/dwm_wl_user_residence/month_id=202404'
    return _userResiPath


# 根据进入时间进行从小到大排序 traces: [(enterTime,regionCode,areaId,tripFlag,leaveTime,provId),,……]
def sortByEnterTime(traces):
    _iTraces = list(traces)
    ##利用_iTraces的key:进入时间戳进行排序，由小到大。
    _sTrace = sorted(_iTraces, key=lambda _iTraces: int(_iTraces[0]))  # sort by enterTime

    ##定义返回List
    retList = []
    ##对已经根据时间排序的数据进行处理，将景区内连续的数据进行合并。
    _slen = len(_sTrace)
    _sTraceList = []
    for _index in range(_slen):
        if _index > 0:
            if list(_sTrace[_index])[0] != list(_sTrace[_index - 1])[0] \
                    and list(_sTrace[_index])[2] != list(_sTrace[_index - 1])[2]:
                ##当前条和上一条对比，如果进入时间不同，景区ID不同，则将当前条加入数组。
                _sTraceList.append(list(_sTrace[_index]))
        else:
            _sTraceList.append(list(_sTrace[_index]))

    _len = len(_sTraceList)
    if _len > 1:
        for _index in range(_len):
            ##行程合并,行程内离开地点和目的地点相同时，
            ##行程有下一个行程
            if _index > 0:
                _currTravel = _sTraceList[_index]
                _prevTravel = _sTraceList[_index - 1]

                ##如果当前行和上一行景区一致，则更新当前行的进入时间[0]，更新上一行为非景区
                if _currTravel[2] == _prevTravel[2] and _currTravel[2] != '0':
                    ##上一条置位非景区，保证一段连续时间每个景区只保留一条记录。
                    _sTraceList[_index - 1][2] = '0'
                    ##更新当前行的景区进入时间。
                    _sTraceList[_index][0] = _sTraceList[_index - 1][0]
                    ##离开时间先打上，防止无离开时间。
                    _sTraceList[_index][4] = _sTraceList[_index][0]
                    _sTraceList[_index][3] = '2'  ##不完整的景区行程，后续判断如果是完整形成会覆盖

                ##上一行是景区且和当前行不同，则更新上一行的离开时间[4]和景区停留表标示[3]
                if _currTravel[2] != _prevTravel[2] and _prevTravel[2] != '0':
                    _sTraceList[_index - 1][3] = '1'
                    _sTraceList[_index - 1][4] = _sTraceList[_index][0]

        ##将非景区行程打标的数据除，只保留景区行程数据。
        for _index in range(_len):
            if int(_sTraceList[_index][3]) != 0:
                retList.append(_sTraceList[_index])
    return tuple(retList)


##将工作地，居住地信息映射成map
def splitUserResidence(line):
    _fields = line.strip().split(",")
    _mdn = _fields[0]
    _residence, _workplace = '0', '0'
    try:
        _residence = str(_fields[1])
        _workplace = str(_fields[2])
    except Exception:
        _residence = '0'
        _workplace = '0'
    # (MDN, (Residence, Workplace))
    return (_mdn, (_residence, _workplace))


def relationWokLive(_trace):
    ##入参的格式 (([arrivalTime, regionCode,areaId,flag,leaveTime,provId]),(居住地,工作地))
    ##将参数转化为List
    _traces = list(_trace)
    ##pos：[(arrivalTime, regionCode,areaId,flag,leaveTime,provId)]
    _pos = list(_traces[0])
    ##_userInfo工作地和常驻地
    _userInfo = _traces[1] if _traces[1] is not None else (0, 0)
    _userList = list(_userInfo)

    ##返回List
    retList = []
    _len = len(_pos)
    for _index in range(_len):
        _posList = list(_pos[_index])
        ##如果行程内的县区ID和用户的工作地居住地匹配，那么就用行程县区ID作为工作地县区ID
        if (_posList[1] in str(_userList[0]) or _posList[1] in str(_userList[1])):
            _posList.append(_posList[1])
            retList.append(_posList)
        ##如果县区ID未匹配到工作地和和居住地，则从工作地和居住地中选取填充
        else:
            _posList.append(str(_userList[1])[0:6])
            retList.append(_posList)
    return retList


def flatTripData(rdd):
    _key = rdd[0]
    _val = [str(x) for x in rdd[1]]
    # return mdn,interTime,leaveTime,stopTimeSecond,regionCode,workRegionCode,areaId,tripFlag
    return _key + ',' + _val[0] + ',' + _val[4] + ',' + str(int(_val[4]) - int(_val[0])) + ',' + _val[1] + \
           ',' + _val[6] + ',' + _val[2] + ',' + _val[3] + ',' + _val[5]


def positionCellKeyMapping(line):
    ##行拆分，根据逗号拆分
    fileds = line.strip().split(",")
    mdn = fileds[0]
    enter_time = fileds[1]
    county_id = fileds[3]
    area_id = fileds[2]
    prov_id = fileds[4]
    # (号码,(进入时间、进入县区CODE、景区ID、景区停留标示、离开景区时间,省份编号))
    return (mdn, (enter_time, county_id, area_id, '0', '0', prov_id))


#########################################################################################
if __name__ == "__main__":
    _dealDate = sys.argv[1]  ##第一个参数处理日期
    ## _provCode = sys.argv[2]  ##第二个参数处理省分

    print (' Deal Date is ' + _dealDate)
    # init SparkContext
    spark = SparkSession.builder.appName("dwm_net_evt_user_scene_day_" + _dealDate).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

    ## 根据_dealDate 计算上一个月账期,获取工作地,居住地没有使用,置为0
    # (MDN, (Residence, Workplace))
    _dateNow = datetime.datetime.strptime(_dealDate, "%Y%m%d")
    _dateMonth = (_dateNow.replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")

    ##工作地和居住地文件地址获取
    _userResPath = getUserResPath(_dealDate)
    ##[(手机号码,(居住地,工作地)),(手机号码,(居住地,工作地)),?]
    userResPairs = sc.textFile(_userResPath, 80).map(splitUserResidence)

    ##加载dwm_wl_user_lbs_hour基础数据
    _userLocationPath = getSrcpath(_dealDate)
    ##格式： (号码,(进入时间,县区CODE,停留时长,景区区域代码,prov_id))
    _userLocation = sc.textFile(_userLocationPath)
    _areaDestPath = getAreaDestPath(_dealDate)

    _userLocation.map(positionCellKeyMapping).groupByKey() \
        .mapValues(sortByEnterTime) \
        .leftOuterJoin(userResPairs) \
        .mapValues(relationWokLive) \
        .flatMapValues(lambda x: x) \
        .map(flatTripData).repartition(16).saveAsTextFile(_areaDestPath)
    spark.sql("msck repair table lbs.dwm_net_evt_user_scene_day sync partitions")
    spark.stop()
