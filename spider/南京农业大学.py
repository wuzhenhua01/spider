# -*- coding:utf-8 -*-

import urllib
import io
from lxml import etree

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    url = 'https://zsxx.njau.edu.cn/lnlqfs.jsp?wbtreeid=1024'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    opener = urllib.URLopener()
    opener.addheader('User-Agent',
                     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')

    resp = opener.open(url)
    content = resp.read()
    tree = etree.HTML(content)
    years = tree.xpath('//select[@id="nf"]/option/text()')
    provinces = tree.xpath('//select[@id="sf"]/option/text()')
    for province in provinces:
        for year in years:
            params = {
                'nf': year,
                'sf': province
            }
            if province == '请选选省份':
                continue
            params = urllib.urlencode(params)
            resp = opener.open(url, params)
            content = resp.read()
            tree = etree.HTML(content)
            lines = tree.xpath('//tbody/tr')
            with io.open('db\\' + province + '.csv', 'spider', encoding='utf-8') as out:
                for idx in range(len(lines)):
                    if idx == 0:
                        continue
                    items = etree.ElementTree(lines[idx])
                    line = ','.join(str(i) for i in items.xpath('//td/text()'))
                    out.write((line + ',' + year).decode('utf-8'))
                    out.write('\n'.decode('utf-8'))
