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

class IEARTHDATATYPE:
    '''导入数据类型'''
    # 智慧城市基础数据
    IEARTHDATA=1
    # 智慧城市区域数据
    IEARA=2
    # 其他数据
    OTHER=0




# 工具配置
class config:
    # DATABASE_URL="postgresql://dev:asdasd123@192.168.1.158/sz_db"
    # DATABASE_URL = "postgresql://dba_iearth:fengdays0105@10.28.11.7/db_iearth_gis"
    DATABASE_URL = "postgresql://dba_iearth:fengdays0105@192.168.1.201/db_iearth_gis"
    Enum = importdatatype()
    IMPORT_DATA_TYPE = Enum.FILEGEODATABASE
    IMPORTFILENAME = 'F:\\temp\\terrain.gdb'
    OPERATETYPE = TOOLOPERATYPE.INSERT
    IMPORT_IEARTHTYPE=IEARTHDATATYPE.IEARA

