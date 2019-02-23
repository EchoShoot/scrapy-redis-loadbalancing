from collections import deque
import time
import math
import logging
from twisted.internet import task
from twisted.internet.threads import deferToThread
from collections import Iterable
from scrapy.utils.reqser import request_to_dict, request_from_dict
from . import picklecompat
from .tools import Color

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, spider, key, serializer=None):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.stats = self.spider.crawler.stats
        self.key = key % {'spider': spider.name}
        self.serializer = serializer

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.delete(self.key)


class RemoteQueue(Base):
    """Per-spider FIFO queue"""

    def __init__(self, *args, **kwargs):
        super(RemoteQueue, self).__init__(*args, **kwargs)
        self.pushDelay = None  # push 命令的访问延迟
        self.popDelay = None  # pop 命令的访问延迟

    @property
    def delay(self):
        """ 返回与服务器通讯延迟 """
        if self.popDelay and self.pushDelay:
            return (self.pushDelay + self.popDelay) / 2
        elif self.popDelay:
            return self.popDelay
        elif self.pushDelay:
            return self.pushDelay
        else:
            return 1

    def __len__(self):
        """Return the length of the queue"""
        return self.server.llen(self.key)

    def push(self, requests):
        """Push a request"""
        if self.stats:  # 只要尝试放都会+1,不管有没有成功
            keypath = 'scheduler/enqueued/{clsname}'.format(clsname=self.__class__.__name__)
            self.stats.inc_value(keypath, spider=self.spider)

        assert isinstance(requests, Iterable), "RemoteQueue 必须提交一个可迭代的对象"

        _start = time.perf_counter()
        pipeline = self.server.pipeline()
        for request in requests:
            pipeline.lpush(self.key, self._encode_request(request))
        result = pipeline.execute()
        _end = time.perf_counter()
        _delay = _end - _start  # 计算延时

        if self.pushDelay:  # 保存延迟
            self.pushDelay = (self.pushDelay + _delay) / 2
        else:
            self.pushDelay = _delay

        logger.info(Color.purple('push {} task to rqueue'.format(len(result))))

    def pop(self, timeout=0, amount=1):  # timeout 是为了兼容接口,未来要去掉的
        """Pop a request"""
        _start = time.perf_counter()
        pipeline = self.server.pipeline()
        for _ in range(int(amount)):
            pipeline.lpop(self.key)
        datas = pipeline.execute()
        _end = time.perf_counter()
        _delay = _end - _start

        if self.popDelay:  # 保存延迟
            self.popDelay = (self.popDelay + _delay) / 2
        else:
            self.popDelay = _delay

        result = [self._decode_request(data) for data in datas if data]
        if result:
            if self.stats:  # 增加计数器
                keypath = 'scheduler/dequeued/{clsname}'.format(clsname=self.__class__.__name__)
                self.stats.inc_value(keypath, spider=self.spider)
            logger.info(Color.purple('pop {} task from rqueue'.format(len(result))))
            return result


class SmartQueue(object):

    def __init__(self, server, spider, key, serializer=None):
        self.lqueue = deque()  # 本地队列
        self.rqueue = RemoteQueue(server, spider, key, serializer)  # 远程队列
        self.stats = spider.crawler.stats
        self.settings = spider.crawler.settings
        self.task = None
        self.interval = 2
        self.__install_list()

    def __install_list(self):
        logger.info(Color.violet('install load_balacing compenont'))
        self.task = task.LoopingCall(self.thread_auto_balacing)  # 定期调用 load_balacing, 利用线程方式
#        self.task = task.LoopingCall(self.auto_balacing)  # 定期调用 load_balacing
        self.task.start(self.interval)

    def __del__(self):
        if self.task and self.task.running:
            self.task.stop()

    @property
    def remoteload(self):
        return len(self.rqueue) / (
                self.settings.getfloat("CONCURRENT_REQUESTS") * (self.stats.get_value('COUNT_OF_HOSTS') or 1))

    @property
    def localload(self):
        tps = self.stats.get_value('tps_page', None)
        if tps:
            return len(self.lqueue) / tps
        else:
            return 0

    def tranfer(self, amount):
        """ 开始转移 - amount:正数为 lqueue->rqueue,负数为 rqueue->lqueue """
        limit = 200
        amount = amount if math.fabs(amount) < limit else limit * amount / math.fabs(amount)
        requests = []
        logger.info(Color.cyan('tranfer:{}'.format(amount)))
        if amount > 0:  # 至少有10个吧!
            try:
                for _ in range(int(amount)):  # 将 amount 任务装入 requests 中
                    request = self.lqueue.pop()
                    requests.append(request)
            except IndexError:
                if requests:
                    self.lqueue.extendleft(requests)  # 如果出错还是放到本地[顺序不变]
            else:
                if requests:
                    self.rqueue.push(requests)  # 没有出错就放到远端
        elif amount < 0:
            try:
                requests = self.rqueue.pop(amount=int(math.fabs(amount)))  # 如果小于 20 从远端拿取
            except Exception:
                if requests:
                    self.rqueue.push(requests)  # 如果错了什么问题,就把任务放回去
                raise  # 继续抛出错误
            finally:
                if requests:
                    self.lqueue.extend(requests)  # 附加到本地队列去

    def thread_auto_balacing(self):
        deferToThread(self.auto_balacing)
    
    def auto_balacing(self):
        restrain = self.stats.get_value('COUNT_OF_HOSTS', 1)  # 约束范围
        remote_tps = self.settings.getfloat("CONCURRENT_REQUESTS")  # 远端吞吐量
        local_tps = self.stats.get_value('tps_page', None) or remote_tps  # 本地吞吐量
        k = self.localload - self.remoteload  # 高度差

        logger.info(Color.red('start to load balacing: {}'.format(k)))

        #        if math.fabs(k) <= restrain and self.remoteload > 1:
        #            if random.uniform(0, 1) < math.fabs(k) / restrain:  # 高度差 < 可控值 -> 概率上传
        #                amount = local_tps*remote_tps*k/(local_tps+remote_tps)
        #                self.tranfer(amount)
        #        else:
        if restrain > 1:
            remote_tps = local_tps * (restrain - 1) + remote_tps
        amount = local_tps * remote_tps * k / (local_tps + remote_tps)
        self.tranfer(amount)

    def push(self, request):
        """ 放入一个任务 """
        self.lqueue.appendleft(request)

    def pop(self, timeout=0):
        """ 弹出一个任务 """
        task = None
        try:
            task = self.lqueue.popleft()
        except IndexError:
            pass
        finally:
            return task

    def clear(self):
        """Clear queue/stack"""
        pass

    def __len__(self):
        """ 队列长度 """
        return len(self.lqueue)
