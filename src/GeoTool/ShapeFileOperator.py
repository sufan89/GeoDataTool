# coding=UTF-8
from ToolOperator import ToolOperator
import os, sys
import ogr
from osgeo import gdal


class ShapeFileOperator(ToolOperator):
    '''shapefile导入操作类'''

    def __init__(self, toolconfig):
        ToolOperator.__init__(self, toolconfig)

    def InsertData(self):
        # 单个文件
        if self.m_Config.IMPORT_DATA_TYPE == 1:
            dataset=self.OpenData(self.m_Config.IMPORTFILENAME)
            layername= os.path.splitext(os.path.basename(self.m_Config.IMPORTFILENAME))
            ToolOperator.InsertData(self,dataset,[layername])
        # ShapeFile文件夹
        elif self.m_Config.IMPORT_DATA_TYPE == 4:
            filenames=self.GetAllShapeFileName(self.m_Config.IMPORTFILENAME)
            for filename in filenames:
                dataset =self.OpenData(filename)
                layername= os.path.splitext(os.path.basename(filename))
                ToolOperator.InsertData(self,dataset,[layername])

    def CreateAndInsert(self):
        # 单个文件
        if self.m_Config.IMPORT_DATA_TYPE == 1:
            dataset=self.OpenData(self.m_Config.IMPORTFILENAME)
            layername= os.path.splitext(os.path.basename(self.m_Config.IMPORTFILENAME))
            ToolOperator.CreateAndInsert(self,dataset,[layername[0]])
        # ShapeFile文件夹
        elif self.m_Config.IMPORT_DATA_TYPE == 4:
            filenames=self.GetAllShapeFileName(self.m_Config.IMPORTFILENAME)
            for filename in filenames:
                dataset =self.OpenData(filename)
                layername= os.path.splitext(os.path.basename(filename))
                ToolOperator.CreateAndInsert(self,dataset,[layername[0]])

    def CreatFormData(self):
        # 单个文件
        if self.m_Config.IMPORT_DATA_TYPE == 1:
            dataset=self.OpenData(self.m_Config.IMPORTFILENAME)
            layername= os.path.splitext(os.path.basename(self.m_Config.IMPORTFILENAME))
            ToolOperator.CreatFormData(self,dataset,[layername[0]])
        # ShapeFile文件夹
        elif self.m_Config.IMPORT_DATA_TYPE == 4:
            filenames=self.GetAllShapeFileName(self.m_Config.IMPORTFILENAME)
            for filename in filenames:
                dataset =self.OpenData(filename)
                layername= os.path.splitext(os.path.basename(filename[0]))
                ToolOperator.CreatFormData(self,dataset,[layername[0]])
    def UpdateData(self):
        pass
    def OpenData(self, filename):
        ogr.RegisterAll()
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO")
        # 读取字段属性值时设置，否则有中文乱码
        gdal.SetConfigOption("SHAPE_ENCODING", "gbk")
        ds = gdal.OpenEx(filename, gdal.OF_VECTOR)
        if ds is None:
            print "Open failed.%s\n" % filename
            sys.exit(1)
        return ds
    def GetAllShapeFileName(self, ShapeFilePath):
        '''获取所有ShapeFile文件名称'''
        List_File = []
        for file in os.listdir(ShapeFilePath):
            file_path = os.path.join(ShapeFilePath, file)
            if os.path.isdir(file_path):
                self.GetAllShapeFileName(file_path)
            elif os.path.splitext(file_path)[1] == '.shp':
                List_File.append(file_path)
        return List_File
