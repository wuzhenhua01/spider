# -*- coding:utf-8 -*-

import urllib
import sys
from lxml import etree
import json

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    opener = urllib.URLopener()
    opener.addheader('User-Agent',
                     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
    provinces = ("台湾", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东",
                 "广西", "海南", "四川", "贵州", "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆", "北京", "天津", "上海", "重庆", "香港", "澳门")
    for province in provinces:
        if province == '台湾':
            continue
        url = 'https://study-cdn.jobpi.cn/index.php?'
        params = {
            'm': 'webInternshipApi_web',
            't': 'enrollMapSet',
            'c': 'index',
            'province_name': province,
            'sch_school_id': 25017
        }
        params = urllib.urlencode(params)
        resp = opener.open(url + params)
        content = resp.read()
        payload = json.loads(content)
        for line in payload['data']['article_data']:
            id = line['id']
            params = {
                'm': 'webInternshipApi_portal',
                't': 'articles',
                'c': 'detail',
                'id': id,
                'sch_school_id': 25017
            }
            params = urllib.urlencode(params)
            resp = opener.open(url + params)
            content = resp.read()
            payload = json.loads(content)
            p = payload['data']['content']
            tree = etree.HTML(content)
            print tree.xpath('//img/@src')[0]

