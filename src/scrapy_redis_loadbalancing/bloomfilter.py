# encoding=utf-8
# ---------------------------------------
#   版本：0.4
#   日期：2018-04-17 19:46:28
#   作者：Echoshoot & 九茶
#   优化了前作者的代码,优化了 BuerFilter 提高效率
# ---------------------------------------

from collections import OrderedDict


class BuerFilter(object):
    """ 存在的目的是,在短期内,不问服务器同样的问题两次!从而减少网络访问的时间损耗 """
    """ 如此我们可以利用一个有序字典! 顺序尾部是最近使用的,顺序头部是很久没用的 """

    def __init__(self, contain=2000):
        self.filter = OrderedDict()  # 去重器
        self.contain = contain  # 约束去重量

    def existent(self, url):
        """ 如果 url 存在返回 True 不存在返回 False """
        if len(self.filter) >= self.contain and self.filter:  # 控制长度
            self.filter.pop(next(self.filter.__iter__()))  # 踢掉最不常用的

        result = self.filter.pop(url, False)
        self.filter[url] = True
        return result


class SimpleHash(object):
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return (self.cap - 1) & ret


class BloomFilter(object):
    def __init__(self, server, key='bloomfilter', blockNum=1, db=0):
        """
        :param server: the client of Redis-Cluster
        :param db: witch db in Redis
        :param blockNum: one blockNum for about 90,000,000; if you have more strings for filtering, increase it.
        :param key: the key's name in Redis
        """
        self.bit_size = 1 << 31  # Redis的String类型最大容量为512M，现使用256M
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.server = server
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        self.buerfilter = BuerFilter(1000)  # 设置容量为 1000
        self.buerfilter_is_on = True  # Buer缓存过滤器
        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    def existent(self, str_input):
        if not str_input:
            return False

        # 如果不二缓存说有,那肯定是爬过,如果不二说没有,那得进一步判断
        if self.buerfilter_is_on and self.buerfilter.existent(str_input):
            return True
        else:
            name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
            # 利用 pipeline 并开启事务,减少RTT次数，提高请求效率。
            pipeline = self.server.pipeline()
            for f in self.hashfunc:
                loc = f.hash(str_input)
                pipeline.setbit(name, loc, 1)
            bool_table = pipeline.execute()
            return all(bool_table)


class OldBloomFilter(object):
    def __init__(self, server, key, blockNum=1):
        self.bit_size = 1 << 31  # Redis的String类型最大容量为512M，现使用256M
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.server = server
        self.key = key
        self.blockNum = blockNum
        self.hashfunc = []
        for seed in self.seeds:
            self.hashfunc.append(SimpleHash(self.bit_size, seed))

    def isContains(self, str_input):
        if not str_input:
            return False
        ret = True

        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(str_input)
            ret = ret & self.server.getbit(name, loc)
        return ret

    def insert(self, str_input):
        name = self.key + str(int(str_input[0:2], 16) % self.blockNum)
        for f in self.hashfunc:
            loc = f.hash(str_input)
            self.server.setbit(name, loc, 1)

    def exists(self, url):
        if self.isContains(url):  # 判断字符串是否存在
            return True
        else:
            self.insert(url)
            return False


if __name__ == '__main__':
    bf = BloomFilter()
    if bf.existent('http://www.baidu.com'):  # 判断字符串是否存在
        print('exists!')
    else:
        print('not exists!')
