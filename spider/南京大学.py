# -*- coding:utf-8 -*-

import urllib
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    opener = urllib.URLopener()
    opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
    opener.addheader('Referer', 'https://bkzs.nju.edu.cn/static/front/nju/basic/html_web/lnfs.html')
    opener.addheader('Cookie', 'zhaosheng.nju.session.id=6b3bbbbe15754a22afb1a93443a8becb')
    opener.addheader('Csrf-Token', '0f2MA')
    opener.addheader('Host', 'bkzs.nju.edu.cn')
    opener.addheader('Origin', 'https', '//bkzs.nju.edu.cn')
    url = 'https://bkzs.nju.edu.cn/f/ajax_lnfs?ts=172042380503'
    params = [('ssmc', '江西'),
              ('zsnf', '2023'),
              ('klmc', '理工'),
              ('zslx', '一般录取')]
    params = urllib.urlencode(params)
    resp = opener.open(url, params)
    content = resp.read()
    print content