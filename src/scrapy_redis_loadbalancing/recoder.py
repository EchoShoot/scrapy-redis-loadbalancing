# coding: utf-8

from __future__ import print_function, unicode_literals
from twisted.internet import task
from collections import deque
import statistics
import uuid
from kazoo.client import KazooClient
import time
import logging
from scrapy import signals
from importlib import import_module
from collections import defaultdict
import sys
import json
from .tools import Color

logger = logging.getLogger(__name__)


class CacheQueue(deque):
    """ 通过 cache 采集一定长度的数据,便于数据分析 """

    def __init__(self, *args, **kwargs):
        super(CacheQueue, self).__init__(*args, **kwargs)
        self.limit = 20  # 30 秒采样时间

    @property
    def mean(self):
        """ 返回平均值 """
        if len(self):
            return statistics.mean(self)

    @property
    def median(self):
        """ 返回中位数 """
        if len(self):
            return statistics.median_high(self)

    @property
    def stdev(self):
        """ 返回标准差 """
        if len(self):
            return statistics.stdev(self)

    def add(self, data):
        if data or len(self):  # 如果data有意义 或 并且队列里面有数据 [0与None没有意义]
            if len(self) >= self.limit:
                self.popleft()
            self.append(data)  # 因为着 队列不为空时, data 即使为 0 也能放入!


class ClusterState(object):
    uuid = uuid.uuid1()  # 生成uuid
    hosts = set()  # 主机信息会存放在这里
    root_path = "/node/host"  # 节点信息存放的根路径

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.crawler = crawler
        self.stats = self.crawler.stats
        self.zk = KazooClient()
        self.__install_listen()  # 安装监听
        self.__install_signal()  # 安装信号

    def __install_signal(self):
        """ 负责把信号对接起来 """
        self.crawler.signals.connect(self.__spider_opened, signal=signals.spider_opened)  # 爬虫开启
        self.crawler.signals.connect(self.__spider_closed, signal=signals.spider_closed)  # 爬虫关闭
        self.crawler.signals.connect(self.__spider_idle, signal=signals.spider_idle)  # 爬虫闲置

    def __install_listen(self):
        """ 集群数量监听: COUNT_OF_HOSTS """

        def listen(children):  # 监听有没有新节点加入
            self.hosts.clear()  # 清空
            self.hosts.update({uuid.UUID(_uuid) for _uuid in children})  # 更新主机列表
            count_of_hosts = len(self.hosts)  # 当前主机的数量
            self.stats.set_value('COUNT_OF_HOSTS', count_of_hosts)
            logger.warning(Color.yellow('COUNT_OF_HOSTS is {}'.format(count_of_hosts)))

        self.zk.ChildrenWatch(self.root_path, func=listen)  # 建立监听

    @property
    def node_name(self):
        """ 获取本节点的名称 """
        return self.uuid.urn

    @property
    def node_path(self):
        """ 获取 zookeeper 中节点保存的路径 """
        return '{path}/{name}'.format(path=self.root_path, name=self.node_name)

    def submit_status(self, data):
        """ 上传该节点的运作状态 """
        bytes_data = json.dumps(data).encode('utf-8')  # json可对象->strings->bytes
        self.zk.set(self.node_path, bytes_data)  # 设置结果

    def __spider_opened(self, spider):
        """ 当爬虫启动: 完成信息注册 """
        self.zk.start()
        self.zk.ensure_path(self.root_path)  # 确保路径存在
        if not self.zk.exists(self.node_path):  # 不存在则创建
            self.zk.create(self.node_path, ephemeral=True, makepath=True)  # 上传节点信息
        logger.info(Color.green("has connected to cluster!"))

    def __spider_closed(self, spider, reason):
        """ 当爬虫关闭: 注销信息注册 """
        if self.zk.exists(self.node_path):  # 如果存在
            self.zk.delete(self.node_path)  # 删除节点信息
        self.zk.stop()
        logger.info(Color.green("has disconnected to cluster!"))

    def __spider_idle(self, spider):
        """ 当爬虫闲置 """
        logger.info(Color.green("self node has idle!"))


class InfoCollection(object):
    """ 用来进行信息参数的收集 """

    def __init__(self):
        try:
            # stdlib's resource module is only available on unix platforms.
            self.resource = import_module('resource')
        except ImportError:
            self.resource = None

    @property
    def memory_usage(self):
        """ 获取内存使用量,非 Unix 系统获得值为 None """
        if self.resource:
            size = self.resource.getrusage(self.resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != 'darwin':
                # on Mac OS X ru_maxrss is in bytes, on Linux it is in KB
                size *= 1024
            return size


class SlotStats(ClusterState):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        super(SlotStats, self).__init__(crawler)  # self.crawler \ self.state \ self.zk
        self.interval = 1
        self.multiplier = 60.0 / self.interval
        self.info_collection = InfoCollection()
        self.__install_signal()  # 安装信号
        self.task = None
        self.start_time = None

        self.prev_dict = {}  # 用来记录前态的字典
        self.prev_buffer_dict = defaultdict(CacheQueue)  # 用来求动态稳定的值

    def __install_signal(self):
        """ 负责把信号对接起来 """
        self.crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        self.crawler.signals.connect(self.request_scheduled, signal=signals.request_scheduled)
        self.crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        """ 周期循环调用 """
        self.task = task.LoopingCall(self.log, spider)  # 循环调用self.log(spider)
        self.task.start(self.interval)  # 设置循环的周期

    def request_scheduled(self, request, spider):
        """ 记住刚开始爬是什么时候 """
        if self.start_time is None:  # 为了方便计算真实吞吐量
            self.start_time = time.perf_counter()

    def spider_closed(self, spider, reason):
        """ 爬虫关闭的时候结束周期循环 """
        if self.task and self.task.running:
            self.task.stop()

        pages = self.stats.get_value('response_received_count', 0)  # 获取下载总量
        if self.start_time:
            total_time = time.perf_counter() - self.start_time  # 计算耗时
            logger.critical(Color.red('The TPS is:{}'.format(pages / total_time)))  # 输出真实的吞吐量

    def discrepancy_calc(self, key, default=0):
        """ 差异计算 """
        current = self.stats.get_value(key, default)  # 获取当期值
        discrepancy = (current - self.prev_dict.get(key, default))  # 当前值-前态值=差异值
        self.prev_dict[key] = current  # 将当前值设为前态值
        return current, discrepancy

    def mean_fix(self, key, value):
        """ 对差异值进行收集,然后求平均值便于稳定其值 """
        self.prev_buffer_dict[key].add(value)
        return self.prev_buffer_dict[key].mean

    def log(self, spider):
        items, item_rate = self.discrepancy_calc('item_scraped_count')
        pages, page_rate = self.discrepancy_calc('response_received_count')
        _, request_bytes = self.discrepancy_calc('downloader/request_bytes')
        _, response_bytes = self.discrepancy_calc('downloader/response_bytes')
        # 吞吐量
        tps_download = (request_bytes + response_bytes)  # 下载的吞吐量
        tps_page = self.mean_fix('response_received_count', page_rate)  # 对值进行稳定
        self.stats.set_value('tps_page', tps_page)  # 稳定后的值作为吞吐量
        length_queue = len(self.crawler.engine.slot.scheduler.queue)  # smartqueue 的队列长度
        # 去重
        buerfilter = self.stats.get_value('dupefilter/buerfilter', 0)  # 不二过滤器去重次数
        bloomfilter = self.stats.get_value('dupefilter/bloomfilter', 0)  # 布隆过滤器去重次数
        # 内存
        memoryusage = self.info_collection.memory_usage  # 内存使用率
        # 队列
        remotequeue = self.stats.get_value('scheduler/dequeued/RemoteQueue', 0) + self.stats.get_value(
            'scheduler/enqueued/RemoteQueue', 0)  # 远程进出队列次数
        smartqueue = self.stats.get_value('scheduler/dequeued/redis', 0) + self.stats.get_value(
            'scheduler/enqueued/redis', 0)  # 本地进出队列次数

        # 数据
        data = {
            'count_pages': pages,  # 爬取量
            'count_items': items,  # 处理量
            'tps_page': tps_page or 0,  # 吞吐量
            'tps_download': tps_download // 1024,  # 转换成KB为单位
            'localload': length_queue / tps_page if tps_page else 0,  # 本地负载量
            'memoryusage': memoryusage / 1024 / 1024,  # MB 内存使用量
            'optimize_filter': (buerfilter - bloomfilter) / buerfilter if buerfilter else 0,  # (本地访问数量-远端访问数量)/本地访问数量
            'optimize_queue': (smartqueue - remotequeue) / smartqueue if smartqueue else 0,  # (本地访问数量-远端访问数量)/本地访问数量
        }

        self.submit_status(data)
        from pprint import pformat
        print(Color.violet(pformat(data)))
        msg = ("Crawled %(pages)d pages (at %(pagerate)d pages/s), "
               "scraped %(items)d items (at %(itemrate)d items/min)")
        log_args = {'pages': pages, 'pagerate': page_rate,
                    'items': items, 'itemrate': item_rate}
        logger.info(Color.cyan(msg), log_args, extra={'spider': spider})
