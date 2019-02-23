class Color(object):
    """ 输出各种颜色,方便 shell观察 """

    @staticmethod
    def black(text):
        """ 黑色 """
        return '\033[90m{content}\033[0m'.format(content=text)

    @staticmethod
    def red(text):
        """ 红色 """
        return '\033[91m{content}\033[0m'.format(content=text)

    @staticmethod
    def green(text):
        """ 绿色 """
        return '\033[92m{content}\033[0m'.format(content=text)

    @staticmethod
    def yellow(text):
        """ 黄色 """
        return '\033[93m{content}\033[0m'.format(content=text)

    @staticmethod
    def violet(text):
        """ 紫罗兰色 """
        return '\033[94m{content}\033[0m'.format(content=text)

    @staticmethod
    def purple(text):
        """ 紫色 """
        return '\033[95m{content}\033[0m'.format(content=text)

    @staticmethod
    def cyan(text):
        """ 青色 """
        return '\033[96m{content}\033[0m'.format(content=text)

    @staticmethod
    def white(text):
        """ 白色 """
        return '\033[97m{content}\033[0m'.format(content=text)

    @staticmethod
    def gray(text):
        """ 灰色 """
        return '\033[98m{content}\033[0m'.format(content=text)