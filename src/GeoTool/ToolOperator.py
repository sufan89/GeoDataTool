# coding=UTF-8
from GeoCommon import DbOperator
import sys


class ToolOperator:
    ''' 文件数据导入数据库基类'''
    m_Config = None
    m_dbOpera = None

    def __init__(self, toolconfig):
        self.m_Config = toolconfig
        self.m_dbOpera = DbOperator(self.m_Config.DATABASE_URL, self.m_Config.IMPORT_DATA_TYPE)

    def CreatFormData(self, dataset=None, layernames=[]):
        '''根据相关数据生成表结构，不导入数据'''
        if dataset is None:
            sys.exit(1)
        LayerCount = dataset.GetLayerCount()
        print "开始创建数据表..."
        for i in range(LayerCount):
            layer = dataset.GetLayer(i)
            layername = layer.GetName()
            if layernames is not None and len(layernames) > 0:
                if layername not in layernames:
                    continue
            insertTable = None
            insertTable = self.m_dbOpera.CreateTable(layer)
            if insertTable is not None:
                print "表:%s创建成功" % layername
            else:
                print "表:%s创建失败" % layername
                continue

    def InsertData(self, dataset=None, layernames=[]):
        '''只导入数据'''
        LayerCount = dataset.GetLayerCount()
        print "开始导入数据..."
        for i in range(LayerCount):
            layer = dataset.GetLayer(i)
            layername = layer.GetName()
            if layernames is not None and len(layernames) > 0:
                if layername not in layernames:
                    continue
            insertTable = None
            insertTable =self.m_dbOpera.GetIearthTahle()
            if insertTable is not None:
                print "表:%s获取成功" % layer.GetName()
            else:
                print "表:%s获取失败" % layer.GetName()
                continue
                #             导入操作
            print "开始对图层:%s进行导入操作" % layer.GetName()
            # self.m_dbOpera.InsertData(insertTable[0], layer, insertTable[1])
            self.m_dbOpera.InsertIearthData(insertTable[0], layer, insertTable[1])

    def CreateAndInsert(self, dataset=None, layernames=[]):
        '''创建数据并导入数据'''
        LayerCount = dataset.GetLayerCount()
        print "开始导入数据..."
        for i in range(LayerCount):
            layer = dataset.GetLayer(i)
            layername = layer.GetName()
            if layernames is not None and len(layernames) > 0:
                if layername not in layernames:
                    continue
            insertTable = None
            insertTable = self.m_dbOpera.CreateTable(layer)
            if insertTable is not None:
                print "表:%s创建成功" % layer.GetName()
            else:
                print "表:%s创建失败" % layer.GetName()
                continue
                #             导入操作
            print "开始对图层:%s进行导入操作" % layer.GetName()
            self.m_dbOpera.InsertData(insertTable[0], layer, insertTable[1])

    def UpdateData(self):
        '''更新数据'''
        pass

    def OpenData(self):
        '''打开数据'''
        pass
    def ExportData(self):
        '''将数据库文件导出'''
        pass
