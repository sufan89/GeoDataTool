# coding=UTF-8
from GeoCommon import DbOperator
from ToolOperator import ToolOperator
from osgeo import gdal
import sys


class GDBOperator(ToolOperator):
    def __init__(self, toolconfig):
        ToolOperator.__init__(self, toolconfig)

    def OpenData(self):
        """打开FileGDB"""
        ds = gdal.OpenEx(self.m_Config.IMPORTFILENAME, gdal.OF_VECTOR)
        if ds is None:
            print "Open FileGeodataBase failed.%s\n" % self.m_Config.IMPORTFILENAME
            sys.exit(1)
        return ds

    def CreatFormData(self):
        dataset = self.OpenData()
        ToolOperator.CreatFormData(self,dataset)

    def InsertData(self):
        dataset = self.OpenData()
        ToolOperator.InsertData(self,dataset)
        pass

    def CreateAndInsert(self):
        dataset = self.OpenData()
        ToolOperator.CreateAndInsert(self, dataset)

    def UpdateData(self):
        pass
