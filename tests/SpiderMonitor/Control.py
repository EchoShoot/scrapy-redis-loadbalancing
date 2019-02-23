from PyQt5.QtWidgets import QApplication
from View import View
from Module import Monitor
import sys


class Control(object):

    def __init__(self):
        self.app = QApplication(sys.argv)
        self.view = View()  # 视图
        # mark 也意味着需要 interval 后才会看到效果
        self.model = Monitor()  # 模型
        self._init()  # 初始化

    def _init(self):
        self.model.Changed.connect(self.view.addItem)
        self.model.Totaled.connect(self.view.updateTotal)
        self.model.Lossed.connect(self.view.delItem)
        self.view.publishTask.connect(self.model.publishTask)

    def launch(self):
        """ 启动 """
        self.view.show()
        self.model.launch()
        sys.exit(self.app.exec_())


def execute():
    """ 执行程序 """
    c = Control()
    c.launch()


if __name__ == '__main__':
    execute()
