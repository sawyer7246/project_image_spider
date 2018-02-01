# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
from scrapy.exceptions import DropItem
import urllib3 as urllib
import os
import time
import random
from sushi.items import *
import re

urllib.disable_warnings(urllib.exceptions.InsecureRequestWarning)


class SushiPipeline(object):
    img_path_labelled = 'D:\STUDY_SOURCE\sushi\img_1\labeled'
    img_path_unlabelled = 'D:\STUDY_SOURCE\sushi\img_1\\unlabelled'
    shop_file_path = 'D:\STUDY_SOURCE\sushi\img_1\shops.jl'
    img_file_path = 'D:\STUDY_SOURCE\sushi\img_1\images.jl'

    seen_urls_path = 'D:\STUDY_SOURCE\sushi\img_1\\visted_url.lines'

    SLEEP_TIME = 5

    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1" \
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]

    def getRandomHeader(self):
        return random.choice(self.user_agent_list)

    def open_spider(self, spider):
        self.shop_file = open(self.shop_file_path, 'w')
        self.img_file = open(self.img_file_path, 'w')

    def close_spider(self, spider):
        self.shop_file.close()
        self.img_file.close()
        self.seen_urls_file.close()

    def __init__(self):
        self.seen_urls_file = open(self.seen_urls_path, 'w')
        self.urls_seen = set()

    # Handle Comment here, return item
    def handleShop(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.shop_file.write(line)

    # Handle profile here, return item
    def handleImage(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.img_file.write(line)

        http = urllib.PoolManager(headers={'User-Agent': self.getRandomHeader()})
        image_urls = item['image_urls']
        for image_url in image_urls:
            time.sleep(random.randint(1, self.SLEEP_TIME))
            res = http.request('GET', image_url['img_url'])
            # print(image_url['img_url'] + '-' + str(res.status))
            sushi_name = self.validate_title(image_url['name'])
            # sushi_hash = str(hash(image_url['img_url']))
            uuid_suffix = image_url['img_url'][-12:]
            file_name = os.path.join(self.img_path_labelled, sushi_name) + '-' + uuid_suffix
            if not self.is_labelled(sushi_name):
                file_name = os.path.join(self.img_path_unlabelled, sushi_name) + '-' + uuid_suffix
            with open(file_name, 'wb') as fp:
                fp.write(res.data)
                print('written file:'+file_name)

    def process_item(self, item, spider):
        if item['url'] in self.urls_seen:
            raise DropItem("Duplicate url found in item: %s" % item)
        else:
            try:
                if isinstance(item, Shop):
                    self.handleShop(item, spider)
                if isinstance(item, Images):
                    self.handleImage(item, spider)
            except Exception as e:
                print('pipeline错误.' + str(e))
            else:
                self.urls_seen.add(item['url'])
                self.seen_urls_file.write(item['url'])

    def validate_title(self, title):
        rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
        new_title = re.sub(rstr, " ", title)  # 替换为下划线
        return new_title

    def is_labelled(self, title):
        arr = re.split('[-]', title)
        if arr[1] is None or arr[1].strip() == '':
            return False
        else:
            return True
