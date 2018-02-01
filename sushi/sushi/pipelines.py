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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'}
    img_path_labelled = 'D:\STUDY_SOURCE\sushi\img_1\labeled'
    img_path_unlabelled = 'D:\STUDY_SOURCE\sushi\img_1\\unlabelled'
    shop_file_path = 'D:\STUDY_SOURCE\sushi\shops.jl'
    img_file_path = 'D:\STUDY_SOURCE\sushi\images.jl'

    SLEEP_TIME = 10

    def open_spider(self, spider):
        self.shop_file = open(self.shop_file_path, 'w')
        self.img_file = open(self.img_file_path, 'w')

    def close_spider(self, spider):
        self.shop_file.close()
        self.img_file.close()

    def __init__(self):
        self.urls_seen = set()

    # Handle Comment here, return item
    def handleShop(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.shop_file.write(line)

    # Handle profile here, return item
    def handleImage(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.img_file.write(line)

        http = urllib.PoolManager(headers=self.headers)
        image_urls = item['image_urls']
        for image_url in image_urls:
            time.sleep(random.randint(1, 5))
            res = http.request('GET', image_url['img_url'])
            # print(image_url['img_url'] + '-' + str(res.status))
            sushi_name = self.validate_title(image_url['name'])
            sushi_hash = str(hash(image_url['img_url']))
            file_name = os.path.join(self.img_path_labelled, sushi_name)+'_'+ sushi_hash + '.jpg'
            if not self.is_labelled(sushi_name):
                file_name = os.path.join(self.img_path_unlabelled, sushi_name)+'_'+ sushi_hash + '.jpg'
            with open(file_name, 'wb') as fp:
                fp.write(res.data)

    def process_item(self, item, spider):
        if item['url'] in self.urls_seen:
            raise DropItem("Duplicate url found in item: %s" % item)
        else:
            self.urls_seen.add(item['url'])
        if isinstance(item, Shop):
            return self.handleShop(item, spider)
        if isinstance(item, Images):
            return self.handleImage(item, spider)

    def validate_title(self, title):
        rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
        new_title = re.sub(rstr, "_", title)  # 替换为下划线
        return new_title

    def is_labelled(self, title):
        arr = re.split('[-]', title)
        if arr[1] is None or arr[1].strip() == '':
            return False
        else:
            return True
