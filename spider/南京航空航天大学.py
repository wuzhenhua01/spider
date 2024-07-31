# -*- coding:utf-8 -*-

import urllib
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    opener = urllib.URLopener()
    opener.addheader('User-Agent',
                     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
    url = 'https://zsservice.nuaa.edu.cn/zsw-admin/api/getAdmissionScore?'
    years = [2021, 2022, 2023]
    provinces = ["台湾", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东",
                 "广西", "海南", "四川", "贵州", "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆", "北京", "天津", "上海", "重庆", "香港", "澳门", ]
    for province in provinces:
        for year in years:
            if province == '台湾':
                continue
            params = [('sf', province),
                      ('year', year),
                      ('page', '1'),
                      ('limit', 500)]
            params = urllib.urlencode(params)
            resp = opener.open(url + params)
            content = resp.read()
            payload = json.loads(content)
            for line in payload['data']:
                print line['specialty']
