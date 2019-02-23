BOT_NAME = 'yaowu'
SPIDER_MODULES = ['yaowu.spiders']
NEWSPIDER_MODULE = 'yaowu.spiders'

SCHEDULER = "scrapy_redis_loadbalancing.scheduler.Scheduler"
#DUPEFILTER_CLASS = "scrapy_redis_loadbalancing.dupefilter.RFPDupeFilter"
DUPEFILTER_CLASS = "scrapy_redis_loadbalancing.dupefilterbloom.BloomDupeFilter"
SCHEDULER_QUEUE_CLASS = 'scrapy_redis_loadbalancing.queues.FifoQueue'
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 8

EXTENSIONS = {
	'scrapy_redis_loadbalancing.recoder.SlotStats': 300,
}

DOWNLOADER_MIDDLEWARES = {
	'scrapy_redis_loadbalancing.downloadermiddlewares.FakeUACP.UserAgentMiddleware':500,
}

ITEM_PIPELINES = {
	'scrapy_redis_loadbalancing.pipelines.RedisPipeline': 300
}