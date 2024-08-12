#!/usr/bin/env python
# -*- coding:utf-8 -*-

from spider.spark_suite_base import SparkSuiteBase


class HudiSparkSQLTest(SparkSuiteBase):
    def insert(self):
        self.spark.sql("""
            CREATE TABLE t132 (
              ts BIGINT,
              uuid STRING,
              rider STRING,
              driver STRING,
              fare DOUBLE,
              city STRING
            ) USING HUDI
            LOCATION 'file:///tmp/t1'
            TBLPROPERTIES(
              type = 'mor',
              primaryKey = 'uuid',
              preCombineField = 'ts'
            )
            PARTITIONED BY (city)
        """)

'''
        self.spark.sql("""
            INSERT INTO t1
            VALUES
            (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19.10, 'san_francisco'),
            (1695091554788, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 27.70, 'san_francisco'),
            (1695046462179, '9909a8b1-2d15-4d3d-8ec9-efc48c536a00', 'rider-D', 'driver-L', 33.90, 'san_francisco'),
            (1695332066204, '1dced545-862b-4ceb-8b43-d2a568f6616b', 'rider-E', 'driver-O', 93.50, 'san_francisco'),
            (1695516137016, 'e3cf430c-889d-4015-bc98-59bdce1e530c', 'rider-F', 'driver-P', 34.15, 'sao_paulo'),
            (1695376420876, '7a84095f-737f-40bc-b62f-6b69664712d2', 'rider-G', 'driver-Q', 43.40, 'sao_paulo'),
            (1695173887231, '3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04', 'rider-I', 'driver-S', 41.06, 'chennai'),
            (1695115999911, 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa', 'rider-J', 'driver-T', 17.85, 'chennai')
        """)

        self.spark.sql("SELECT ts, uuid, fare, rider, driver, city FROM t1 WHERE fare > 20.0").show(False)
'''

if __name__ == '__main__':
    test = HudiSparkSQLTest()
    test.insert()
