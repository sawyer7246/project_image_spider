from scrapy import cmdline

# cmdline.execute("scrapy crawl sushi".split())
cmdline.execute("scrapy crawl sushi -s JOBDIR=crawls/somespider-1".split())