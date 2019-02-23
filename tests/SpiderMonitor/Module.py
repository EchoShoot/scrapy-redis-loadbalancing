from kazoo.client import KazooClient
import uuid
from scrapy_redis_loadbalancing.tools import Color
import logging
import time
from pprint import pprint
import statistics
import redis
import os
from collections import Counter
import json
from PyQt5.QtCore import pyqtSignal, QObject, pyqtSlot, QTimer

logger = logging.getLogger(__name__)


class Monitor(QObject):
    hosts = set()  # 主机信息会存放在这里
    root_path = "/node/host"  # 节点信息存放的根路径
    log_path = os.path.join(os.getcwd(), "log.json")
    Changed = pyqtSignal(dict)  # 数据
    Totaled = pyqtSignal(dict)
    Lossed = pyqtSignal(set)  # 丢失主机

    def __init__(self, **kwargs):
        super(Monitor, self).__init__(**kwargs)
        self.interval = 1000
        self.timer = None
        self.zk = KazooClient()
        self.redis = redis.StrictRedis()
        self.__install_listen()  # 安装监听
        self.zk.start()
        self.file = open(self.log_path,'a+')

    def __del__(self):
        self.zk.stop()
        self.file.close()

    def __install_listen(self):
        """ 集群数量监听: COUNT_OF_HOSTS """

        def listen(children):  # 监听有没有新节点加入
            old_hosts = self.hosts.copy()  # 备份,方便比对
            self.hosts.clear()  # 清空
            self.hosts.update({uuid.UUID(_uuid) for _uuid in children})  # 更新主机列表
            count_of_hosts = len(self.hosts)  # 当前主机的数量
            logger.warning(Color.yellow('COUNT_OF_HOSTS is {}'.format(count_of_hosts)))
            if len(old_hosts) > len(self.hosts):
                self.Lossed.emit(old_hosts - self.hosts)  # 警报丢失主机

        self.zk.ChildrenWatch(self.root_path, func=listen)  # 建立监听

    def get_path(self, node_name):
        """ 获取 zookeeper 中节点保存的路径 """
        return '{path}/{name}'.format(path=self.root_path, name=node_name)

    @pyqtSlot()
    def get_info(self):
        xxx = Counter()
        loss_balacing = []
        for host in self.hosts:
            result, _ = self.zk.get(self.get_path(host.urn))
            result = result.decode('utf-8')
            if result:
                result = json.loads(result)
                loss_balacing.append(result.get('localload'))  # 负载量 localload 累积起来
                xxx.update(result)  # Counter 的 update 是累加的
                result['node_name'] = host.urn
                self.Changed.emit(result)
        else:
            xxx['node_counts'] = len(self.hosts)
            xxx['memoryusage'] /= len(self.hosts)  # 内存占用取平均值
            xxx['optimize_filter'] /= len(self.hosts)  # 去重优化率取平均值
            xxx['optimize_queue'] /= len(self.hosts)  # 队列优化率取平均值
            xxx['loss_balacing'] = 0 if len(loss_balacing) < 2 else statistics.stdev(loss_balacing) # 负载失衡为负载量取方差
            self.Totaled.emit(xxx)
            self.file.write(json.dumps(xxx)+',')  # 存入文件中
            self.file.flush()

    @pyqtSlot(str, str)
    def publishTask(self, spidername, urlseed):
        """ 发布任务 """
        try:
            self.redis.lpush("{}:start_urls".format(spidername), urlseed)
        except:
            return False
        else:
            return True

    def launch(self):
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.get_info)
        self.timer.start(self.interval)  # 每秒钟调用一次


if __name__ == "__main__":
    monitor = Monitor()
    monitor.launch()
