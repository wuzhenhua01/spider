#!/usr/bin/env python
# -*- coding:utf-8 -*-


class Loggable:
    def __init__(self):
        pass

    def get_logger(self, spark, custom_prefix):
        log4j_logger = spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(custom_prefix + self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__
