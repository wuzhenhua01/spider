# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession. \
    builder. \
    appName("sync_dwm_lbs_evt_position_compress_hour"). \
    enableHiveSupport(). \
    getOrCreate()


def main():
    hudi_options = {
        'hoodie.metadata.enable': 'false',
        'hoodie.table.name': 'dwm_lbs_evt_position_compress_hour',
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        'hoodie.datasource.write.partitionpath.field': 'day_id,hour_id',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.meta.sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': 'lbs',
        'hoodie.datasource.hive_sync.table': 'dwm_lbs_evt_position_compress_hour',
        'hoodie.datasource.hive_sync.mode': 'jdbc',
        'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://10.37.69.1:2181/lbs;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_zk;principal=hive/_HOST@HADOOP.CHINATELECOM.CN'
    }

    inserts_df = spark.read.orc('hdfs://nma07-302-h12-sev-r4900-2u39:8020/domain/ns3/wznlxt_yx/dwm_db.db/dwm_lbs_evt_position_compress_hour/day_id=20240717/hour_id=00'). \
        withColumn('day_id', lit('20240717')). \
        withColumn('hour_id', lit('00'))
    inserts_df.write.format('hudi'). \
        options(**hudi_options). \
        mode('append'). \
        save('/user/hive/warehouse/lbs/dwm_lbs_evt_position_compress_hour_1')


if __name__ == '__main__':
    main()
