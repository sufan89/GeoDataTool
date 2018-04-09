# coding=UTF-8
from Importgdb import gdbImport

'''导入工厂'''


class Importfactory:
    def __init__(self, toolconfig):
        # shapefile单个文件
        if toolconfig.IMPORT_DATA_TYPE == 1:
            return gdbImport(toolconfig)
        # GDB数据库
        elif toolconfig.IMPORT_DATA_TYPE == 2:
            return
        # MDB数据库
        elif toolconfig.IMPORT_DATA_TYPE == 3:
            return
        # shapfile文件夹
        elif toolconfig.IMPORT_DATA_TYPE == 4:
            return
        # 栅格数据
        elif toolconfig.IMPORT_DATA_TYPE == 5:
            return
        # 其他数据格式
        elif toolconfig.IMPORT_DATA_TYPE == 6:
            return
