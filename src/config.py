# coding=UTF-8


# 导入数据类型
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


# 工具操作类型
class TOOLOPERATYPE:
    # 只导入
    INSERT = 1
    # 创建表并导入
    CREATRANDINSERT = 2
    # 更新
    UPDATE = 3
    # 导出
    EXPORT = 4
    # 创建表结构
    CREATE = 5
    # 创建GIS物体对象
    CREATEOBJECT = 6


# 工具配置
class config:
    DATABASE_URL = ""
    Enum = importdatatype()
    IMPORT_DATA_TYPE = Enum.FILEGEODATABASE
    IMPORTFILENAME = ''
    OPERATETYPE = TOOLOPERATYPE.INSERT
    MainTableName = ""
