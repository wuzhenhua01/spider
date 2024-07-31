# -*- coding:utf-8 -*-

import pymongo
myclient = pymongo.MongoClient("mongodb://xueyingying.com:27017")
mydb = myclient["d1"]
mycol = mydb["c1"]
mydict = {"_id": 1, "platform": "微信公众号", "name": "愤怒的it男", "introduce": "人生苦短，Python是岸！"}
x = mycol.insert_one(mydict)
print(x.inserted_id)
