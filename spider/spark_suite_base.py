#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from spider.loggable import Loggable


class SparkSuiteBase(Loggable):
    def __init__(self):
        os.environ['SPARK_CONF_DIR'] = '.'
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hudi:hudi-spark2.4-bundle_2.11:0.14.0 pyspark-shell"

        Loggable.__init__(self)

        self.spark_conf = SparkConf(False)
        self.spark = SparkSession.builder \
            .master("local") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .getOrCreate()

        print self.spark.conf.get("spark.sql.extensions")
        self.log = self.get_logger(self.spark, "datalake")
