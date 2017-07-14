from sqlalchemy import create_engine,Table,Column,Integer,String,MetaData
from geoalchemy2 import Geometry
import sys,ogr
from osgeo import gdal

def openShp(filename,layername):
    ds = gdal.OpenEx(filename, gdal.OF_VECTOR)
    if ds is None:
        print "Open failed.%s\n"%filename
        sys.exit(1)
    lyr = ds.GetLayerByName(layername)
    lyr.ResetReading()

    for feat in lyr:
        print "**********************************"
        print feat.ExportToJson()
        feat_defn = lyr.GetLayerDefn()
        for i in range(feat_defn.GetFieldCount()):
            field_defn = feat_defn.GetFieldDefn(i)
            # Tests below can be simplified with just :
            # print feat.GetField(i)
            if field_defn.GetType() == ogr.OFTInteger or field_defn.GetType() == ogr.OFTInteger64:
                print "%d" % feat.GetFieldAsInteger64(i)
            elif field_defn.GetType() == ogr.OFTReal:
                print "%.3f" % feat.GetFieldAsDouble(i)
            elif field_defn.GetType() == ogr.OFTString:
                print "%s" % feat.GetFieldAsString(i)
            else:
                print "%s" % feat.GetFieldAsString(i)
        geom = feat.GetGeometryRef()
        if geom is not None :
            if geom.GetGeometryType()==ogr.wkbPoint or geom.GetGeometryType()==ogr.wkbMultiPoint:
                 print "point X:%.3f,point Y: %.3f" % (geom.GetX(), geom.GetY())
                 print geom.ExportToWkt()
           # print "%.3f, %.3f" % (geom.GetX(), geom.GetY())
        else:
            print "no  geometry\n"
    ds = None
def open_postgisdb():
    engine = create_engine('postgresql://postgres:sufan2008300379@localhost/dbgeoserver', echo=True)
    metadata = MetaData()
    lake_tabl=Table('lake',metadata,Column('id',Integer,primary_key=True),Column('name',String),Column('geom',Geometry(geometry_type='POLYGON', srid=4326)))
    lake_tabl.create(engine)
    ins = lake_tabl.insert()
    print str(ins)
    ins=lake_tabl.insert().values(name="Majeur",geom='SRID=4326;POLYGON((0 0,1 0,1 1,0 1,0 0))')
    print str(ins)
    # ins.compile.params()
    conn=engine.connect()
    result=conn.execute(ins)
    result.inserted_primary_key
    conn.execute(lake_tabl.insert(),name='sufan',geom='SRID=4326;POLYGON((0 0,1 0,1 1,0 1,0 0))')



if __name__=="__main__":
    # engine = create_engine("postgresql://posgres:sufan2008300379@127.0.0.1/dbgeoserver", echo=True)
    #  openShp("E:\\test\\data_shp\\DLGK_HYD_PT.shp","DLGK_HYD_PT")
     open_postgisdb()
   # print engine
