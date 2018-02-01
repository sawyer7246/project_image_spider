# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Shop(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    url = scrapy.Field()
    location = scrapy.Field()
    price_range_day = scrapy.Field()
    price_range_night = scrapy.Field()
    rate_day = scrapy.Field()
    rate_night = scrapy.Field()
    comments_cnt = scrapy.Field()


class Images(scrapy.Item):
    # define the fields for your item here like:
    #shop_url
    url = scrapy.Field()
    image_urls = scrapy.Field()
