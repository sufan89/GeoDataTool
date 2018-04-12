# coding=UTF-8
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, LargeBinary, DateTime, BigInteger, \
    Float
from sqlalchemy.orm import sessionmaker
from osgeo import ogr
from geoalchemy2 import Geometry

Base = declarative_base()


class DbOperator:
    '''数据库操作类'''
    m_connect = ''
    m_engine = None
    m_session = None
    m_importType = 0
    m_metadata = None

    def __init__(self, strconnect, importType):
        self.m_connect = strconnect
        self.m_engine = create_engine(strconnect, echo=False)
        Session = sessionmaker(bind=self.m_engine)
        self.m_session = Session()
        self.m_importType = importType
        self.m_metadata = MetaData()

    def CreateTable(self, importlayer):
        '''根据图层信息创建表'''
        newtable = self.GetTable(importlayer)
        newtable[0].create(self.m_engine)
        self.m_metadata.create_all(self.m_engine)
        return newtable

    def getTableColumn(self, field_defn):
        '''根据数据列获取数据库列信息'''
        field_type = field_defn.GetType()
        field_name = field_defn.GetName().lower()
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

    def GetGeometryColumn(self, geometry_defn, Srid_Value):
        ''' 获取Geometry列信息'''
        geometry_type = geometry_defn.GetGeomType()
        if geometry_type == ogr.wkbPoint:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value))
            else:
                return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value))
        elif geometry_type == ogr.wkbPointM or geometry_type == ogr.wkbPoint25D:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTIPOINT', srid=Srid_Value, dimension=3, management=True))
            else:
                return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbPointZM:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTIPOINT', srid=Srid_Value, dimension=4, management=True))
            else:
                return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbMultiPoint:
            return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value))
        elif geometry_type == ogr.wkbMultiPointM or geometry_type == ogr.wkbMultiPoint25D:
            return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbMultiPointZM:
            return Column('geom', Geometry(geometry_type='MULTIPOINT', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbLineString:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom', Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value))
            else:
                return Column('geom', Geometry(geometry_type='LINESTRING', srid=Srid_Value))
        elif geometry_type == ogr.wkbLineStringM or geometry_type == ogr.wkbLineString25D:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value, dimension=3, management=True))
            else:
                return Column('geom',
                              Geometry(geometry_type='LINESTRING', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbLineStringZM:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value, dimension=4, management=True))
            else:
                return Column('geom',
                              Geometry(geometry_type='LINESTRING', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbMultiLineString:
            return Column('geom', Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value))
        elif geometry_type == ogr.wkbMultiLineStringM or geometry_type == ogr.wkbMultiLineString25D:
            return Column('geom',
                          Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbMultiLineStringZM:
            return Column('geom',
                          Geometry(geometry_type='MULTILINESTRING', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbPolygon:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value))
            else:
                return Column('geom', Geometry(geometry_type='POLYGON', srid=Srid_Value))
        elif geometry_type == ogr.wkbPolygonM or geometry_type == ogr.wkbPolygon25D:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value, dimension=3, management=True))
            else:
                return Column('geom', Geometry(geometry_type='POLYGON', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbPolygonZM:
            if self.m_importType == 1 or self.m_importType == 4:
                return Column('geom',
                              Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value, dimension=4, management=True))
            else:
                return Column('geom', Geometry(geometry_type='POLYGON', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbMultiPolygon:
            return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value))
        elif geometry_type == ogr.wkbMultiPolygonM or geometry_type == ogr.wkbMultiPolygon25D:
            return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value, dimension=3, management=True))
        elif geometry_type == ogr.wkbMultiPolygonZM:
            return Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=Srid_Value, dimension=4, management=True))
        elif geometry_type == ogr.wkbGeometryCollection:
            return Column('geom', Geometry(geometry_type='GEOMETRYCOLLECTION', srid=Srid_Value))
        elif geometry_type == ogr.wkbCurve:
            return Column('geom', Geometry(geometry_type='CURVE', srid=Srid_Value))
        else:
            return Column('geom', Geometry(geometry_type='POINT', srid=Srid_Value))

    def GetTable(self, importlayer):
        '''根据图层信息创建表对象'''
        if importlayer is None:
            print "无法创建表，无图层信息"
        # 创建表
        feat_defn = importlayer.GetLayerDefn()
        geometry_defn = feat_defn.GetGeomFieldDefn(0)
        spatial_ref = geometry_defn.GetSpatialRef()
        #  查询数据库表来获取Srid
        out_spatial = self.m_session.query(Spatial).filter_by(proj4text=spatial_ref.ExportToProj4()).first()
        srid = -1
        if out_spatial is not None:
            srid = out_spatial.auth_srid
        fieldCount = feat_defn.GetFieldCount()
        newtable = Table(importlayer.GetName().lower(), self.m_metadata)
        newtable.append_column(Column('fid', Integer, primary_key=True))
        for i in range(fieldCount):
            newtable.append_column(self.getTableColumn(feat_defn.GetFieldDefn(i)))
        # 设置图形字段
        newtable.append_column(self.GetGeometryColumn(feat_defn, srid))
        return [newtable, srid]

    def InsertData(self, insertTable, imLayer, srid=-1):
        """向数据库中插入数据"""
        featDic = {}
        conn = self.m_engine.connect()
        feat_defn = imLayer.GetLayerDefn()
        fieldCount = feat_defn.GetFieldCount()
        insertCount = 0
        totalInsertCount = 0
        insetData = []
        featureCount = imLayer.GetFeatureCount()
        for feat in imLayer:
            for i in range(fieldCount):
                field_defn = feat_defn.GetFieldDefn(i)
                featDic[field_defn.GetName().lower()] = feat.GetField(i)
            if feat.GetFID() is not None:
                featDic['fid'] = feat.GetFID()
            # 获取图形信息
            geom = feat.GetGeometryRef()
            wkt_geom = geom.ExportToIsoWkt()
            if srid != -1:
                featDic['geom'] = 'SRID=' + str(srid) + ';' + wkt_geom
            else:
                featDic['geom'] = wkt_geom
            insetData.append(featDic.copy())
            featDic.clear()
            insertCount = insertCount + 1
            totalInsertCount = totalInsertCount + 1
            insertper = totalInsertCount / (featureCount * 1.0) * 100
            if insertCount == 1000:
                conn.execute(insertTable.insert(), insetData)
                insertCount = 0
                insetData=[]
                print "已完成导入：%s" % round(insertper, 2)
        if insertCount != 0:
            conn.execute(insertTable.insert(), insetData)
        print "完成对图层：%s的导入" % imLayer.GetName()


class Spatial(Base):
    '''坐标系类'''
    __tablename__ = 'spatial_ref_sys'
    srid = Column(Integer, primary_key=True)
    auth_name = Column(String(256))
    auth_srid = Column(Integer)
    srtext = Column(String(2048))
    proj4text = Column(String(2048))
