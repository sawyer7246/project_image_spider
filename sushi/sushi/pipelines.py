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
import datetime
import codecs

urllib.disable_warnings(urllib.exceptions.InsecureRequestWarning)


class SushiPipeline(object):
    root_path = '../data/'
    img_path_labelled = root_path+'sushi/img_{0}/labeled/'
    img_path_unlabelled = root_path+'sushi/img_{0}/unlabelled/'
    shop_file_path = root_path+'sushi/img_{0}/'
    shop_file_name = 'shops.jl'
    img_file_path = root_path+'sushi/img_{0}/'
    img_file_name = 'images.jl'

    seen_urls_path = root_path+'sushi/'
    seen_urls_name = 'visted_url.lines'

    SLEEP_TIME = 5

    FLUSH_PARA = 3
    COUNT = 1

    DATE_STR = ""

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
        self.open_files()

    def close_files(self):
        self.shop_file.close()
        self.img_file.close()
        self.seen_urls_file.close()

    def make_dir(self, File_Path):
        if not os.path.exists(File_Path):
            os.makedirs(File_Path)

    def open_files(self):
        self.make_dir(self.shop_file_path.format(self.DATE_STR))
        self.make_dir(self.img_file_path.format(self.DATE_STR))
        self.make_dir(self.seen_urls_path.format(self.DATE_STR))

        self.make_dir(self.img_path_labelled.format(self.DATE_STR))
        self.make_dir(self.img_path_unlabelled.format(self.DATE_STR))
        self.shop_file = codecs.open(self.shop_file_path.format(self.DATE_STR) + self.shop_file_name, 'w+', encoding='utf8')
        self.img_file = codecs.open(self.img_file_path.format(self.DATE_STR) + self.img_file_name, 'w+', encoding='utf8')
        self.seen_urls_file = codecs.open(self.seen_urls_path + self.seen_urls_name, 'a+', encoding='utf8')

    def close_spider(self, spider):
        self.close_files()

    def __init__(self):
        self.DATE_STR = datetime.datetime.now().strftime('%Y-%m-%d')
        self.urls_seen = set()
        self.open_files()
        self.seen_urls_file.seek(0)
        for i, url in enumerate(self.seen_urls_file.readlines()):
            self.urls_seen.add(url.replace("\n", ""))
        print('Load '+str(len(self.urls_seen))+' visited urls...')


    # Handle Comment here, return item
    def handleShop(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.shop_file.write(line)
        self.COUNT = 1 + self.COUNT

    # Handle profile here, return item
    def handleImage(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.img_file.write(line)

        http = urllib.PoolManager(headers={'User-Agent': self.getRandomHeader()})
        image_urls = item['image_urls']
        for image_url in image_urls:
            self.COUNT = 1 + self.COUNT
            tmp_url = image_url['img_url']
            time.sleep(random.randint(1, self.SLEEP_TIME))
            res = http.request('GET', tmp_url)
            # print(image_url['img_url'] + '-' + str(res.status))
            sushi_name = self.validate_title(image_url['name'])
            # sushi_hash = str(hash(image_url['img_url']))
            uuid_suffix = tmp_url[-12:]
            file_name = os.path.join(self.img_path_labelled.format(self.DATE_STR), sushi_name) + '-' + uuid_suffix
            if not self.is_labelled(sushi_name):
                file_name = os.path.join(self.img_path_unlabelled.format(self.DATE_STR), sushi_name) + '-' + uuid_suffix
            with open(file_name, 'wb') as fp:
                fp.write(res.data)
                print('written file:'+file_name)
            self.urls_seen.add(tmp_url+ "\n")
            self.seen_urls_file.write(tmp_url + "\n")

    def process_item(self, item, spider):
        self.COUNT = 1 + self.COUNT
        new_date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        if self.COUNT % self.FLUSH_PARA == 0:
            self.close_files()
            self.open_files()
            print('Reset file success!')
        if new_date_str != self.DATE_STR:
            self.DATE_STR = new_date_str
            self.close_files()
            self.open_files()
            print('Reset file success!')

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
                self.urls_seen.add(item['url'] + "\n")
                self.seen_urls_file.write(item['url'] + "\n")
                return

    def validate_title(self, title):
        rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
        new_title = re.sub(rstr, " ", title)  # 替换为下划线
        return new_title.replace("\r\n", " ").replace("\n", " ").replace("/", " ")

    def is_labelled(self, title):
        arr = re.split('[-]', title)
        if arr[1] is None or arr[1].strip() == '':
            return False
        else:
            return True
