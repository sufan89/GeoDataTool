# coding=UTF-8



class importdatatype:
    def __init__(self):
        pass

    # shapefile单个文件
    SHAPEFILE = 1
    # GDB数据库
    FILEGEODATABASE = 2
    # MDB数据库
    PERSONALGEODATABASE = 3
    # shapfile文件夹
    SHAPEFILEPATH = 4
    # 栅格数据
    RASTER = 5
    # 其他数据格式
    OTHER = 0


class TOOLOPERATYPE:
    INSERT = 1
    CREATRANDINSERT = 2
    UPDATE = 3
    EXPORT = 4
    CREATE = 5


class config:
    DATABASE_URL = ""
    Enum = importdatatype()
    IMPORT_DATA_TYPE = Enum.SHAPEFILEPATH
    IMPORTFILENAME = ''
    OPERATETYPE = TOOLOPERATYPE.INSERT
