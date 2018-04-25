# coding=UTF-8
from GDBOperator import GDBOperator
from ShapeFileOperator import ShapeFileOperator
from RasterOperator import RasterOperator


class OperatorFactory:
    '''导入工厂'''
    m_ToolOperator = None

    def __init__(self, toolconfig):
        # shapefile单个文件
        if toolconfig.IMPORT_DATA_TYPE == 1:
            self.m_ToolOperator = ShapeFileOperator(toolconfig)
        # GDB数据库
        elif toolconfig.IMPORT_DATA_TYPE == 2:
            self.m_ToolOperator = GDBOperator(toolconfig)
        # MDB数据库
        elif toolconfig.IMPORT_DATA_TYPE == 3:
            self.m_ToolOperator = GDBOperator(toolconfig)
        # shapfile文件夹
        elif toolconfig.IMPORT_DATA_TYPE == 4:
            self.m_ToolOperator = ShapeFileOperator(toolconfig)
        # 栅格数据
        elif toolconfig.IMPORT_DATA_TYPE == 5:
            self.m_ToolOperator = RasterOperator(toolconfig)
        # 其他数据格式
        elif toolconfig.IMPORT_DATA_TYPE == 6:
            self.m_ToolOperator = GDBOperator(toolconfig)
