import logging
import time

from scrapy.dupefilters import BaseDupeFilter
from scrapy_redis_loadbalancing import defaults
from scrapy_redis_loadbalancing.bloomfilter import BloomFilter
from scrapy_redis_loadbalancing.connection import get_redis_from_settings
from scrapy.dupefilters import request_fingerprint

logger = logging.getLogger(__name__)


# TODO: Rename class to BloomDupeFilter. "must end with DupeFilter!"
class BloomDupeFilter(BaseDupeFilter):
    """Redis-based request duplicates filter.

    This class can also be used with default Scrapy's scheduler.

    """

    logger = logger

    def __init__(self, server, key, stats, debug=False):
        """Initialize the duplicates filter.

        Parameters
        ----------
        server : redis.StrictRedis
            The redis server instance.
        key : str
            Redis key Where to store fingerprints.
        debug : bool, optional
            Whether to log filtered requests.

        """
        import redis
        self.server = server
#        self.server = redis.StrictRedis()
        self.key = key
        self.stats = stats
        self.debug = debug
        self.logdupes = True
        self.bf = BloomFilter(self.server, key, blockNum=1)

    @classmethod
    def from_settings(cls, settings, stats):
        """Returns an instance from given settings.

        This uses by default the key ``dupefilter:<timestamp>``. When using the
        ``scrapy_redis_loadbalancing.scheduler.Scheduler`` class, this method is not used as
        it needs to pass the spider name in the key.

        Parameters
        ----------
        settings : scrapy.settings.Settings

        Returns
        -------
        RFPDupeFilter
            A RFPDupeFilter instance.


        """
        server = get_redis_from_settings(settings)
        # XXX: This creates one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        # TODO: Use SCRAPY_JOB env as default and fallback to timestamp.
        key = defaults.DUPEFILTER_KEY % {'timestamp': int(time.time())}
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, key=key, stats=stats, debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        """Returns instance from crawler.

        Parameters
        ----------
        crawler : scrapy.crawler.Crawler

        Returns
        -------
        RFPDupeFilter
            Instance of RFPDupeFilter.

        """
        return cls.from_settings(crawler.settings, crawler.stats)

    @classmethod
    def from_spider(cls, spider):
        settings = spider.settings
        server = get_redis_from_settings(settings)
        dupefilter_key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY)
        key = dupefilter_key % {'spider': spider.name}
        debug = settings.getbool('DUPEFILTER_DEBUG')
        stats = spider.crawler.stats
        return cls(server, key=key, stats=stats, debug=debug)

    def request_seen(self, request):
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        # BloomFilter
        self.stats.inc_value('dupefilter/buerfilter')
        if self.bf.existent(fp):
            return True
        else:
            self.stats.inc_value('dupefilter/bloomfilter')
            return False
            # This returns the number of values added, zero if already exists.
            # added = self.server.sadd(self.key, fp)
            # return added == 0

    def request_fingerprint(self, request):
        """Returns a fingerprint for a given request.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        str

        """
        # splash_request_fingerprint 会自动判断 request 是否符合 SplashRequest 特征
        # 如果符合 SplashRequest 特征会进一步处理,否则就和普通的 request_fingerprint 是一样的效果
        return request_fingerprint(request)

    def close(self, reason=''):
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()

    def clear(self):
        """Clears fingerprints data."""
        self.server.delete(self.key)

    def log(self, request, spider):
        """Logs given request.

        Parameters
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False
