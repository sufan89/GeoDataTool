# coding=UTF-8
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, LargeBinary, DateTime, BigInteger, \
    Float
from geoalchemy2 import Geometry
import sys, ogr, os
from osgeo import gdal
from config import config, importdatatype
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
engine = create_engine(config.DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

class Spatial(Base):
    __tablename__ = 'spatial_ref_sys'
    srid = Column(Integer, primary_key=True)
    auth_name = Column(String(256))
    auth_srid = Column(Integer)
    srtext = Column(String(2048))
    proj4text = Column(String(2048))


def GetAllShapeFileName(ShapeFilePath):
    List_File = []
    for file in os.listdir(ShapeFilePath):
        file_path = os.path.join(ShapeFilePath, file)
        if os.path.isdir(file_path):
            GetAllShapeFileName(file_path)
        elif os.path.splitext(file_path)[1] == '.shp':
            List_File.append(file_path)
    return List_File


def openshapefile(filename):
    ds = gdal.OpenEx(filename, gdal.OF_VECTOR)
    if ds is None:
        print "Open failed.%s\n" % filename
        sys.exit(1)
        os.path.split()
    return ds


def runtool():
    # 单个ShapeFile导入
    if config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILE:
        ImportShapeFile(config.IMPORTFILENAME)
    # ShapeFile 文件夹导入
    elif config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILEPATH:
        ShapeFileNames = GetAllShapeFileName(config.IMPORTFILENAME)
        if len(ShapeFileNames) == 0:
            print "This folder has no shapefiles!"
            return
        else:
            for ShapeFileName in ShapeFileNames:
                ImportShapeFile(ShapeFileName)


def ImportShapeFile(ShapeFileName):
    layername = os.path.splitext(os.path.basename(ShapeFileName))
    dataset = openshapefile(ShapeFileName)
    data_layer = dataset.GetLayerByName(layername[0])
    #        创建表
    feat_defn = data_layer.GetLayerDefn()
    geometry_defn = feat_defn.GetGeomFieldDefn(0)
    spatial_ref = geometry_defn.GetSpatialRef()
    #  查询数据库表来获取Srid
    out_spatial = session.query(Spatial).filter_by(proj4text=spatial_ref.ExportToProj4()).first()
    srid = -1
    if out_spatial is not None:
        srid = out_spatial.auth_srid
    fieldCount = feat_defn.GetFieldCount()
    metadata = MetaData()
    newtable = Table(layername[0].lower(), metadata)
    newtable.append_column(Column('fid', Integer, primary_key=True))
    for i in range(fieldCount):
        newtable.append_column(getTableColumn(feat_defn.GetFieldDefn(i)))
    # 设置图形字段
    newtable.append_column(GetGeometryColumn(feat_defn, srid))
    newtable.create(engine)
    metadata.create_all(engine)
    #         读取数据，并存入数据库中
    featDic = {}
    conn = engine.connect()
    for feat in data_layer:
        for i in range(fieldCount):
            field_defn = feat_defn.GetFieldDefn(i)
            featDic[field_defn.GetName().lower()] = feat.GetField(i)
        if feat.GetFID() is not None:
            featDic['fid'] = feat.GetFID()
        # 获取图形信息
        geom = feat.GetGeometryRef()
        wkt_geom = getNewGeometry(geom).ExportToWkt()
        if srid != -1:
            featDic['geom'] = 'SRID=' + str(srid) + ';' + wkt_geom
        else:
            featDic['geom'] = wkt_geom
        conn.execute(newtable.insert(), [featDic])


# 将单点，单线，单面转换成多点、多线、多面
def getNewGeometry(GeomtryRef):
    if GeomtryRef.GetGeometryType() == ogr.wkbPoint:
        NewGeometry = ogr.Geometry(ogr.wkbMultiPoint)
        NewGeometry.AddGeometry(GeomtryRef)
        return NewGeometry
    elif GeomtryRef.GetGeometryType() == ogr.wkbLineString:
        NewGeometry = ogr.Geometry(ogr.wkbMultiLineString)
        NewGeometry.AddGeometry(GeomtryRef)
        return NewGeometry
    elif GeomtryRef.GetGeometryType() == ogr.wkbPolygon:
        NewGeometry = ogr.Geometry(ogr.wkbMultiPolygon)
        NewGeometry.AddGeometry(GeomtryRef)
        return NewGeometry
    else:
        return GeomtryRef


def getTableColumn(field_defn):
    field_type = field_defn.GetType()
    field_name = field_defn.GetName()
    if field_type == ogr.OFTBinary:
        return Column(field_name, LargeBinary)
    elif field_type == ogr.OFTDate:
        return Column(field_name, Date)
    elif field_type == ogr.OFTDateTime:
        return Column(field_name, DateTime)
    elif field_type == ogr.OFTInteger:
        return Column(field_name, Integer)
    elif field_type == ogr.OFTInteger64:
        return Column(field_name, BigInteger)
    elif field_type == ogr.OFTReal:
        return Column(field_name, Float)
    elif field_type == ogr.OFTString:
        return Column(field_name, String(field_defn.GetWidth()))


def GetGeometryColumn(geometry_defn, Srid_Value):
    geometry_type = geometry_defn.GetGeomType()
    if geometry_type == ogr.wkbPoint:
        if config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILEPATH or config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILE:
            return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value))
        else:
            return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value))
    elif geometry_type == ogr.wkbMultiPoint:
        return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value))
    elif geometry_type == ogr.wkbLineString:
        if config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILEPATH or config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILE:
            return Column('geom', Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value))
        else:
            return Column('geom', Geometry(geometry_type='LINESTRING', srid=Srid_Value))
    elif geometry_type == ogr.wkbMultiLineString:
        return Column('geom', Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value))
    elif geometry_type == ogr.wkbPolygon:
        if config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILEPATH or config.IMPORT_DATA_TYPE == importdatatype.SHAPEFILE:
            return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value))
        else:
            return Column('geom', Geometry(geometry_type='POLYGON', srid=Srid_Value))
    elif geometry_type == ogr.wkbMultiPolygon:
        return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value))
    elif geometry_type == ogr.wkbGeometryCollection:
        return Column('geom', Geometry(geometry_type='GEOMETRYCOLLECTION', srid=Srid_Value))
    elif geometry_type == ogr.wkbCurve:
        return Column('geom', Geometry(geometry_type='CURVE', srid=Srid_Value))
    else:
        return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value))


if __name__ == "__main__":
    runtool()
