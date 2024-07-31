# coding=utf-8
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
import sys, time, io

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf8", line_buffering=True)
filterTime = 0
# 定义剔除的异常手机号码（这些号码轨迹量巨大，数据倾斜严重）。
expMDN = ['17300002161',
          '18900000000',
          '13300000000',
          '17717376170',
          '17744960736',
          '13308877524',
          '16212191494',
          '18922097814',
          '17317917030']


def getAreaConfDataPath():
    _path = 'hdfs://hucang/user/hive/warehouse/lbs/dim_area_attraction_config/*/*'
    # _path = '/user/hive/warehouse/lbs/hudi/dim_area_attraction_config/*'
    return _path


# hudi表配置
def areaConfDataMap_hudi(line):
    _confData = line
    _cellKey = str(_confData[8])
    _provCode = str(_confData[7])[0:2]
    _key = _provCode + "_" + _cellKey
    _id = str(_confData[5])
    return (_key, _id)


def areaConfDataMap(line):
    _confData = line.strip().split(",")
    _cellKey = str(_confData[3])
    _provCode = str(_confData[2])[0:2]
    _key = _provCode + "_" + _cellKey
    _id = str(_confData[0])
    return (_key, _id)


# 根据基站ID关联景点ID
def areaIdMap(line):
    # print('@开始关联景点名')
    _mdn = str(line.msisdn)
    _enterTime = str(line.occur_time)[0:10]
    _countyCode = str(line.county_code)
    _cellKey = str(line.cell_key)
    _provCode = str(line.prov_code)
    _key = _provCode[0:2] + "_" + _cellKey
    _tmpAreaId = str(_boardcastAreaConfData.value.get(_key))
    _areaId = _tmpAreaId if _tmpAreaId != 'None' else '0'
    # print('@关联景点名完成')
    return (_mdn, (_enterTime, _countyCode, _areaId, _provCode))


def areaSortAndFilter(values):
    # print('@开始过滤景区数据')
    _traces = list(values)
    # 根据进入时间对数据进行排序，由早到晚
    _sortedTraces = sorted(_traces, key=lambda _traces: (int(_traces[0]), int(_traces[1])))

    _resultList = []
    # 如果当前条数据有景区ID,且进入时间和景区ID都与上一条不同，则保留
    _len = len(_sortedTraces)
    for _index in range(_len):
        if _index > 0:
            if _sortedTraces[_index][0] != _sortedTraces[_index - 1][0] \
                    and _sortedTraces[_index][2] != _sortedTraces[_index - 1][2]:
                _resultList.append(_sortedTraces[_index])
        else:
            _resultList.append(_sortedTraces[_index])
    # print('@景区数据过滤完成')
    return _resultList


# 如果过滤后mdn对应的value没有数据，则删除此条数据
def filterNoneData(rdd):
    _var = list(rdd)
    if len(_var[1]) == 0:
        return False
    else:
        return True


# 将(mdn,(enterTime,regionCode,areaId,provCode))格式的数据转换为(mdn,enterTime,regionCode,areaId,provCode)
def flatAreaData(rdd):
    # print('@开始导出景区数据写入hdfs格式')
    _mdn = rdd[0]
    _values = [str(x) for x in rdd[1]]
    # print('@景区数据导出完成')
    # 最终的数据格式为mdn,enter_time,area_id,county_code
    return _mdn + ',' + _values[0] + ',' + _values[2] + ',' + _values[1] + ',' + _values[3]


def filterData2(line):
    _cellKey = str(line.cell_key)
    _mdn = str(line.msisdn)
    _enterTime = str(line.occur_time)[0:10]
    _countyCode = str(line.county_code)
    if not (str(_enterTime).isdigit() and str(_countyCode).isdigit()
            and len(str(_enterTime)) >= 10 and len(str(_countyCode)) == 6):
        return False
    ##号码不等于11位、非1开头、物联网号码则丢弃
    # if (len(str(_mdn)) != 11 or str(_mdn)[0] != '1' or str(_mdn)[0:2] == '10'
    #    or str(_mdn)[0:4] == '1410' or str(_mdn)[0:4] == '1441'):
    #    return False
    if len(_cellKey) < 5:
        return False
    if _mdn in expMDN:
        return False
    return True


def lengthFilter(line):
    _confData = line.strip().split(",")
    _confData = line
    if len(_confData) >= 4:
        return True
    return False


if __name__ == "__main__":
    _params = sys.argv[1:]
    _dealTime = _params[0]
    _dealDate = _dealTime[:8]
    _hour = _dealTime[8:10]
    filterTime = int(time.mktime(time.strptime(_dealDate + '' + _hour + '0000', '%Y%m%d%H%M%S')))

    print('@@' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('@@开始配置sc')
    spark = SparkSession.builder.appName(
        "dwm_net_evt_user_scene_hour_(area)" + _dealDate + "_" + _hour).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("INFO")
    print('@@sc配置完成')

    # 获取天小时粒度数据
    print('@@开始获取天小时粒度数据')
    _locationData_0 = spark.sql(
        "select msisdn,county_code,occur_time,cell_key,prov_code from lbs.dwm_lbs_evt_position_compress_hour where day_id=" + _dealDate + " and hour_id=" + _hour)
    print('@@获取天小时粒度数据完成')

    # 获取景区配置数据存储路径
    _areaConfDataPath = getAreaConfDataPath()
    # 获取景区配置数据并将格式转换为(基站编码,景区ID),然后根据key进行去重
    _areaConfData = sc.textFile(_areaConfDataPath).filter(lengthFilter).map(areaConfDataMap).collectAsMap()
    # _areaConfData = spark.read.format("hudi").load(_areaConfDataPath).rdd.map(areaConfDataMap_hudi).collectAsMap()
    print('@@获取景区配置数据完成')

    # 广播处理后的景区配置数据
    _boardcastAreaConfData = sc.broadcast(_areaConfData)
    print('@@广播完成')
    print('@@' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    _areaPath = "hdfs://hucang/user/hive/warehouse/lbs.db/dwm_net_evt_user_scene_hour/day_id=" + _dealDate + "/area_type=area/hour_id=" + _hour + "/"

    # 将数据关联上景区ID,没有关联上的数据过滤掉，同一人同景区的连续重复数据过滤掉，仅保留一条
    _locationData_1 = _locationData_0.rdd.\
        filter(filterData2).\
        map(areaIdMap).\
        groupByKey().\
        mapValues(areaSortAndFilter).\
        filter(filterNoneData).\
        flatMapValues(lambda x: x).\
        map(flatAreaData).\
        repartition(8).\
        saveAsTextFile(_areaPath)
    print('@@景区数据写入完成')
    print('@@' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    spark.sql("msck repair table lbs.dwm_net_evt_user_scene_hour sync partitions")
    spark.stop()
