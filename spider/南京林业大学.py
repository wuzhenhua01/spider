# -*- coding:utf-8 -*-

import urllib
import json
from lxml import etree

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    url = 'https://lqcx.njfu.edu.cn/page/score-overyear.html'

    opener = urllib.URLopener()
    opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')

    resp = opener.open(url)
    content = resp.read()
    tree = etree.HTML(content)
    years = tree.xpath('//select[@id="yearInfo"]/option/text()')
    provinces = tree.xpath('//select[@id="province"]/option/@value')
    for province in provinces:
        for year in years:
            if year == '2024' or year == '2023':
                continue
            url = 'https://lqcx.njfu.edu.cn/enrollment/open/scoreList?'
            params = [('year', year), ('provinceName', province), ('kind', '')]
            params = urllib.urlencode(params)
            resp = opener.open(url + params)
            content = resp.read()
            payload = json.loads(content)
            if payload['code'] == 1:
                continue
            for line in payload['data']:
                print line['majorName'] \
                    + line['kind'] \
                    + line['maxScore'] \
                    + line['minScore'] \
                    + line['batch'] \
                    + str(line['provinceScore']) \
                    + line['year'] \
                    + str(line['provinceName'])

