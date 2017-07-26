# coding=UTF-8
from sqlalchemy import create_engine,Table,Column,Integer,String,MetaData,Date,LargeBinary,DateTime,BigInteger,Float
from geoalchemy2 import Geometry
import sys,ogr,os
from osgeo import gdal
from osgeo.osr import  SpatialReference
from config import config,importdatatype
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime
Base = declarative_base()

class Spatial(Base):
    __tablename__='spatial_ref_sys'
    srid=Column(Integer,primary_key=True)
    auth_name=Column(String(256))
    auth_srid=Column(Integer)
    srtext=Column(String(2048))
    proj4text=Column(String(2048))

def openshapefile(filename):
    ds = gdal.OpenEx(filename, gdal.OF_VECTOR)
    if ds is None:
        print "Open failed.%s\n"%filename
        sys.exit(1)
        os.path.split()
    return ds


def runtool():
    engine = create_engine(config.DATABASE_URL, echo=True)
    Session = sessionmaker(bind=engine)
    session=Session()
    # 单个ShapeFile导入
    if config.IMPORT_DATA_TYPE==importdatatype.SHAPEFILE:
        layername = os.path.splitext(os.path.basename(config.IMPORTFILENAME))
        dataset=openshapefile(config.IMPORTFILENAME)
        data_layer=dataset.GetLayerByName(layername[0])
#        创建表
        feat_defn = data_layer.GetLayerDefn()
        geometry_defn = feat_defn.GetGeomFieldDefn(0)
        spatial_ref=geometry_defn.GetSpatialRef()
        #  查询数据库表来获取Srid
        out_spatial= session.query(Spatial).filter_by(proj4text=spatial_ref.ExportToProj4()).first()
        srid=-1
        if out_spatial is not None:
            srid=out_spatial.auth_srid
        fieldCount=feat_defn.GetFieldCount()
        metadata = MetaData()
        newtable=Table(layername[0].lower(),metadata)
        newtable.append_column(Column('FID',Integer,primary_key=True))
        for i in range(fieldCount):
            newtable.append_column(getTableColumn(feat_defn.GetFieldDefn(i)))
#         设置图形字段
        newtable.append_column(GetGeometryColumn(feat_defn,srid))
        newtable.create(engine)
        metadata.create_all(engine)
#         读取数据，并存入数据库中
        featDic={}
        conn = engine.connect()
        for feat in data_layer:
            for i in range(fieldCount):
                field_defn = feat_defn.GetFieldDefn(i)
                featDic[field_defn.GetName()] = feat.GetField(i)
            if feat.GetFID() is not None:
                featDic['FID']=feat.GetFID()
#             获取图形信息
            geom=feat.GetGeometryRef()
            wkt_geom=geom.ExportToWkt()
            if srid != -1:
                featDic['geom'] = 'SRID='+str(srid)+';'+wkt_geom
            else:
                featDic['geom'] = wkt_geom
            conn.execute(newtable.insert(),[featDic])
    # ShapeFile 文件夹导入
    elif config.IMPORT_DATA_TYPE==importdatatype.SHAPEFILEPATH:


def getTableColumn(field_defn):
    field_type=field_defn.GetType()
    field_name=field_defn.GetName()
    if field_type == ogr.OFTBinary:
        return Column(field_name,LargeBinary)
    elif field_type == ogr.OFTDate:
        return Column(field_name,Date)
    elif field_type == ogr.OFTDateTime:
        return Column(field_name,DateTime)
    elif field_type == ogr.OFTInteger:
        return Column(field_name,Integer)
    elif field_type == ogr.OFTInteger64:
        return  Column(field_name,BigInteger)
    elif field_type == ogr.OFTReal:
        return  Column(field_name,Float)
    elif field_type == ogr.OFTString:
        return  Column(field_name,String(field_defn.GetWidth()))

def GetGeometryColumn(geometry_defn,Srid_Value):
    geometry_type=geometry_defn.GetGeomType()
    if geometry_type==ogr.wkbPoint:
        return Column('geom',Geometry(geometry_type='POINT', srid=Srid_Value))
    elif geometry_type==ogr.wkbMultiPoint:
        return Column('geom',Geometry(geometry_type='MULTIPOINT', srid=Srid_Value))
    elif geometry_type==ogr.wkbLineString:
        return Column('geom',Geometry(geometry_type='LINESTRING', srid=Srid_Value))
    elif geometry_type == ogr.wkbMultiLineString:
        return Column('geom',Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value))
    elif geometry_type == ogr.wkbPolygon:
        return Column('geom',Geometry(geometry_type='POLYGON', srid=Srid_Value))
    elif geometry_type == ogr.wkbMultiPolygon:
        return Column('geom',Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value))
    elif geometry_type == ogr.wkbGeometryCollection:
        return Column('geom',Geometry(geometry_type='GEOMETRYCOLLECTION', srid=Srid_Value))
    elif geometry_type == ogr.wkbCurve:
        return Column('geom',Geometry(geometry_type='CURVE', srid=Srid_Value))
    else:
        return Column('geom',Geometry(geometry_type='POINT', srid=Srid_Value))

if __name__=="__main__":
    runtool()