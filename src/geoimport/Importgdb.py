# coding=UTF-8
from toolimport import toolimport
from osgeo import gdal
import sys
from geocommon import dboperate

'''GDB数据库导入操作'''


class gdbImport(toolimport):
    def runimport(self):
        dataset = self.OpenFileGeodatabase()
        LayerCount = dataset.GetLayerCount()
        print "开始导入数据..."
        for i in range(LayerCount):
            layer = dataset.GetLayer(i)
            opDb = dboperate(self.m_Config.DATABASE_URL, self.m_Config.IMPORT_DATA_TYPE)
            insertTable=None
            if self.m_Config.OPERATETYPE == 2 or self.m_Config.OPERATETYPE == 5:
                insertTable = opDb.CreateTable(layer)
                if insertTable is not None:
                    print "表:%s创建成功" % layer.GetName()
            if self.m_Config.OPERATETYPE==1 or self.m_Config.OPERATETYPE==3:
                pass
        pass

    # 打开FileGDB
    def OpenFileGeodatabase(self):
        ds = gdal.OpenEx(self.m_Config.IMPORTFILENAME, gdal.OF_VECTOR)
        if ds is None:
            print "Open FileGeodataBase failed.%s\n" % self.m_Config.IMPORTFILENAME
            sys.exit(1)
        return ds
