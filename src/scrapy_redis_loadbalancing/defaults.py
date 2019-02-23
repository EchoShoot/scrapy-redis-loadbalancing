import redis

# For standalone use.
DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'

PIPELINE_KEY = '%(spider)s:items'

REDIS_CLS = redis.StrictRedis
REDIS_ENCODING = 'utf-8'
# Sane connection defaults.
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': REDIS_ENCODING,
}

SCHEDULER_QUEUE_KEY = '%(spider)s:requests'
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis_loadbalancing.queues.FifoQueue'
SCHEDULER_QUEUE_CLASS = 'scrapy_redis_loadbalancing.smartqueue.SmartQueue'
SCHEDULER_DUPEFILTER_KEY = '%(spider)s:dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_redis_loadbalancing.dupefilterbloom.BloomDupeFilter'

START_URLS_KEY = '%(name)s:start_urls'
START_URLS_AS_SET = False
