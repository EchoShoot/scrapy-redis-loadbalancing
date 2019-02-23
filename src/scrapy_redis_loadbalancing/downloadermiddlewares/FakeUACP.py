"""Set User-Agent header per spider or use a default value from settings"""

from scrapy import signals
from .UAFakeData import get_user_agent_by_random


class UserAgentMiddleware(object):
    """This middleware allows spiders to override the user_agent"""

    def __init__(self, UA_type=None):
        self.UA_type = UA_type or 'PC'

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler.settings['UA_TYPE'])
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        return o

    def spider_opened(self, spider):
        self.UA_type = getattr(spider, 'ua_type', self.UA_type)

    def process_request(self, request, spider):
        user_agent = get_user_agent_by_random(self.UA_type)
        request.headers[b'User-Agent'] = user_agent