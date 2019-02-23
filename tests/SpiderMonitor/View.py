import UI_main
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QTableWidgetItem, QTableWidget, QMenu
from PyQt5.QtCore import pyqtSlot, Qt, pyqtSignal
import numbers
from PyQt5.QtGui import QBrush, QColor
import logging
import re
from collections import OrderedDict

# logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class View(QMainWindow, UI_main.Ui_MainWindow):
    publishTask = pyqtSignal(str, str)

    def __init__(self, *args, **kwargs):
        super(View, self).__init__(*args, **kwargs)
        self.setupUi(self)  # 安装UI
        self.__install_table()
        self.__install_signal()

    def __install_signal(self):
        """ 安装信号槽 """

        def sendTask():
            spidername = self.spidernameEdit.text().strip()  # 蜘蛛名
            urlseed = self.urlseedEdit.text().strip()  # URL种子
            self.publishTask.emit(spidername, urlseed)  # 触发信号

        self.publishButton.clicked.connect(sendTask)  # 发送任务

    def __install_table(self):
        """ 安装表格的配置信息 """
        self.TABLE = OrderedDict({
            '负载压力': 'localload',
            '处理速度': 'tps_page',
            '下载速度': 'tps_download',
            '内存占用': 'memoryusage',
            '处理数量': 'count_items',
            '抓取页面': 'count_pages',
            '节点名称': 'node_name',
        })
        self.tableWidget.setColumnCount(len(self.TABLE))
        self.tableWidget.setHorizontalHeaderLabels(self.TABLE.keys())
        self.tableWidget.setEditTriggers(QTableWidget.NoEditTriggers)  # 不允许修改内容
        self.tableWidget.setSelectionBehavior(QTableWidget.SelectRows)  # 点击就是选择一行
        self.tableWidget.setSelectionMode(QTableWidget.SingleSelection)  # 个可选
        self.tableWidget.resizeColumnsToContents()  # 垂直自适应调整
        self.tableWidget.resizeRowsToContents()  # 水平自适应调整

    @staticmethod
    def yieldItem(value):
        if isinstance(value, numbers.Real):
            obj = QTableWidgetItem()
            obj.setData(Qt.DisplayRole, value)
        else:
            obj = QTableWidgetItem(value)
        return obj

    @pyqtSlot(dict)
    def updateTotal(self, data):
        pages = data.get(self.TABLE['抓取页面'], 0)
        tps = data.get(self.TABLE['处理速度'], 0)
        items = data.get(self.TABLE['处理数量'], 0)
        speed = data.get(self.TABLE['下载速度'], 0)
        loss_balacing = data.get('loss_balacing', 0)
        node_counts = data.get('node_counts', 0)
        self.loss_balacing.setText("{}".format(round(loss_balacing, 3)))
        self.node_counts.setText("{}个".format(node_counts))
        self.pages.setText("{}个".format(pages))
        self.tps.setText("{}个/s".format(round(tps, 1)))
        self.items.setText("{}个".format(items))
        self.speed.setText("{}KB/s".format(round(speed, 1)))

    @pyqtSlot(dict)  # 数据字典
    def addItem(self, data):
        """ 添加一项到列表 """
        item = self.tableWidget.findItems(data.get(self.TABLE['节点名称']), Qt.MatchExactly)
        if item:
            index = self.tableWidget.indexFromItem(item[0])
            index = index.row()
        else:
            self.tableWidget.insertRow(0)  # 新建一行
            index = 0

        for column, value in enumerate(self.TABLE.values()):
            self.tableWidget.setItem(index, column, self.yieldItem(data.get(value, 0)))  # 插入 路径
        self.tableWidget.resizeColumnsToContents()  # 依据文字大小进行调整
        self.tableWidget.sortItems(1, Qt.DescendingOrder)  # 按 size 排序

    @pyqtSlot(set)
    def delItem(self, host_names):
        for host_name in host_names:
            result = self.tableWidget.findItems(host_name.urn, Qt.MatchExactly)
            for each in result:
                index = self.tableWidget.indexFromItem(each)
                self.tableWidget.removeRow(index.row())
                self.tableWidget.sortItems(1, Qt.DescendingOrder)  # 按 size 排序
