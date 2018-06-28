from .ToolOperator import ToolOperator
import os, sys
import ogr
from osgeo import gdal

class RasterOperator(ToolOperator):
    def __index__(self,toolconfig):
        ToolOperator.__init__(self,toolconfig)
    def OpenData(self):
        ds=gdal.OpenEx(self.m_Config.IMPORTFILENAME, gdal.OF_RASTER)
        if ds is None:
            print ("Open FileGeodataBase failed.%s\n" % self.m_Config.IMPORTFILENAME)
            sys.exit(1)
        return ds

    def CreateAndInsert(self):
        ds=self.OpenData()
        