from sushi.items import *
import scrapy


class SushiSpider(scrapy.Spider):

    name = "sushi"

    def start_requests(self):
        urls = [
            'https://tabelog.com/cn/rstLst/1/?lat=36.2088&lon=137.6367&zoom=12&RdoCosTp=2&LstCos=0&LstCosT=12&LstSitu'
            '=0&LstRev=0&LstReserve=0&ChkParking=0&LstSmoking=0&SrtT=rt&LstCatSD=RC010202&LstCatD=RC0102&LstCat=RC01'
            '&Cat=RC',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for item in response.css(".js-list-item"):
            temp_shop = Shop()
            temp_shop['name'] = item.css(".js-detail-anchor::text").extract_first()
            temp_shop['url'] = item.css('a::attr("href")').extract_first()
            detail_page = temp_shop['url']
            yield scrapy.Request(detail_page+'dtlphotolst/1/1/?smp=s', callback=self.image_list_parse)
            temp_shop['location'] = item.css(".list-rst__name-ja::text").extract_first()
            temp_shop['rate_day'] = item.css(".c-rating--val35 .c-rating__time--lunch+ .c-rating__val::text").extract_first()
            temp_shop['rate_night'] = item.css(".c-rating--val35 .c-rating__time--dinner+ .c-rating__val::text").extract_first()
            temp_shop['price_range_day'] = item.css(".list-rst__price .c-rating__time--lunch+ .c-rating__val::text").extract_first()
            temp_shop['price_range_night'] = item.css(".list-rst__price .c-rating__time--dinner+ .c-rating__val::text").extract_first()
            temp_shop['comments_cnt'] = item.css(".list-rst__reviews-target b::text").extract_first()
            yield temp_shop
        next_page = response.css('.c-pagination__target--next::attr(href)').extract_first()
        if next_page is not None:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
        else:
            return

    def image_list_parse(self, response):
        # https: // tabelog.com / cn / miyagi / A0404 / A040404 / 4003018 /
        # https: // tabelog.com / cn / miyagi / A0404 / A040404 / 4003018 / dtlphotolst / 1 / 1 /?smp = s
        shop_url = response.url
        image_urls = []
        temp_img = Images()
        temp_img['url'] = shop_url
        temp_img['image_urls'] = image_urls
        for img in response.css(".rd-grids__photo-img"):
            alt = img.css("img::attr(alt)").extract_first()
            img_url = img.css("a::attr(href)").extract_first()
            image_urls.append({'img_url': img_url, 'name': alt})
        yield temp_img
        # .c - pagination__list: nth - child(7).c - pagination__target
        next_page = response.css('.c-pagination__target[rel="next"]::attr(href)').extract_first()
        if next_page is not None:
            yield scrapy.Request(response.urljoin(next_page), callback=self.image_list_parse)
        else:
            return
