# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 22:46:41 2020

@author: D071127
"""

# encoding: utf-8

import platform
platform.architecture()
import sys
from psycopg2 import connect
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

con = None
con = connect(user='postgres', host='localhost', password='postgres', database='kuppingen')
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


def getMaxTauschabend(conn):
    result =0
    curLI = conn.cursor()
    curLI.execute("SELECT MAX(tauschabende_id) FROM tausch;")
    result = curLI.fetchall()
    curLI.close()
    result = str(result[0])
    result.split(",")
    return result[1]+result[2]

cur = con.cursor()
#cur.execute("SELECT * from geo_flurstuecke inner join tausch on (geo_flurstuecke.gid = tausch.flurstuecke_id) WHERE tausch.tauschabende_id IN (SELECT MAX(id) FROM tauschabende);")
#Select mit st_dump führt schläge zusammen + join auf den letzten Tauschabend für einen speziellen bewirtschafter
cur.execute("SELECT ST_AsText((ST_DUMP(ST_UNION(geom))).geom), bewirtschafter_id FROM geo_flurstuecke g inner join tausch on (g.gid = tausch.flurstuecke_id) INNER JOIN flurstuecke f ON(f.geo_flurstuecke_id = g.gid) WHERE tausch.bewirtschafter_id = 188 and tausch.tauschabende_id ="+getMaxTauschabend(con)+"and f.nutzung LIKE '%Acker%' GROUP BY bewirtschafter_id;")
       
     #rückgabewert für neue Schlaege wird in neue variable geschrieben
neueSchlaege = cur.fetchall()
cur.close()
former = con.cursor()
former.execute("SELECT ST_AsText((ST_DUMP(ST_UNION(geom))).geom), bewirtschafter_id FROM geo_flurstuecke g inner join tausch on (g.gid = tausch.flurstuecke_id) INNER JOIN flurstuecke f ON(f.geo_flurstuecke_id = g.gid) WHERE tausch.bewirtschafter_id = 188 and tausch.tauschabende_id = 1 and f.nutzung LIKE '%Acker%' GROUP BY bewirtschafter_id;")
       
alteSchlaege = former.fetchall()
tauschmax = getMaxTauschabend(con)
con.close()
former.close()

#VARIANTE A)


# CO2 Einspartnis = Dieseleinsparnis

# Dieseleinsparnis = Summe Diesel / Betrieb vorher - Summe Diesel/Betrieb nachher
# = größe durchschnittsgröße vorher * schläge - durschnittsgröße nachher* schläge
# 1. durchschnittsgröße ( dieselverbrauch ) anhand KTBL Kurve ermitteln --> Kurve interpolieren
# 2. Für einen Bewirtschafter summe fläche / Schläge vorher errechnen

# VARIANTE B)
# SUMME (Schlaggröße * Interpolationswert aus KTBL für Diesel) VORHER - SUMME(Schlaggröße * Interpolationswert aus KTBL für Diesel) NACHHER
# PROBLEM AKTUELL:
# 1. Transformation von Flurstücke in Schlage ( noch nicht da )
# 2. Liste aus SQL befehlen ( von Jens anfordern )

## import geopanda for area berechnung

import pandas as pd
# dataframe basteln aus neueSchlaege py list
dfObjpd=pd.DataFrame(neueSchlaege, columns =['geometry', 'bewirtschafterID'])
##das selbe für den initialen schlag stand bei tauschabend min
dfObjpdformer=pd.DataFrame(alteSchlaege, columns=['geometry', 'bewirtschafterID'] )
#print (dfObj)
import geopandas as gpd
# = gpd.read_file(dfObj)
#print (dfObj.crs)


from shapely import wkt
import shapely.wkt
#dfObj = dfObjpd
dfObjpd['geometry'] = dfObjpd['geometry'].map(shapely.wkt.loads)

dfObjpdformer = dfObjpdformer['geometry'].map(shapely.wkt.loads)

gdf = gpd.GeoDataFrame(dfObjpd, geometry ='geometry')
gdfformer = gpd.GeoDataFrame(dfObjpdformer, geometry ='geometry')

gdf.crs = "EPSG:31467"
gdfformer.crs = "EPSG:31467"

## Berechne Schlaggröße des zusammengefassten Schlages auf basis neuestem Tauschabend
gdf["area"] = gdf['geometry'].area/ 10**4
gdf.head(2)


#Kosten per Schlag nachher = area * xx
gdf["kosten"]= (0.4313*gdf["area"]**4 - 8.8792*gdf["area"]**3 + 66.062*gdf["area"]**2 - 223.83*gdf["area"] + 809.24)*gdf["area"]

#TBD: BewirtschafterMatrix mit kosten per bewirtschafter
#bewirtschafter[]

#for n in gdf['bewirtschafter']:
 #   bewirtschafter[n] = gdf['bewirtschafter'.sum().kosten
#summe aller kosten per bewirtschfter
schlaggroeseavg=gdf.sum().area/gdf.index.size


gdfgesamtkosten = gdf.sum().kosten

gdfgesamthektar = gdf.sum().area

kostenperarea = gdfgesamtkosten/gdfgesamthektar

#summe bewirtschafter nachher - vorher
## STand vorher:
gdfformer["area"] = gdfformer['geometry'].area/ 10**4

#Kosten per Schlag nachher = area * xx
gdfformer["kosten"]= (0.4313*gdfformer["area"]**4 - 8.8792*gdfformer["area"]**3 + 66.062*gdfformer["area"]**2 - 223.83*gdfformer["area"] + 809.24)*gdf["area"]

#summe aller kosten per bewirtschfter
schlaggroeseavgformer=gdfformer.sum().area/gdfformer.index.size
gdfgesamtkostenformer = gdfformer.sum().kosten

gdfgesamthektarformer = gdfformer.sum().area

kostenperareaformer = gdfgesamtkostenformer/gdfgesamthektarformer

Ersparnis = gdfgesamtkostenformer - gdfgesamtkosten

#jeden Schlag Umfang rechnen

#gdf["Umfang"]= gdf['geometry'].

gdf["diesel"]= (5.3254*gdf["area"]**2- 0.463*gdf["area"]**3 - 22.897*gdf["area"] + 156.67)*gdf["area"]
gdfformer["diesel"] = (-0.463*gdfformer["area"]**3 + 5.3254*gdfformer["area"]**2 - 22.897*gdfformer["area"] + 156.67)*gdfformer["area"]

Dieselersparnis = gdfformer.sum().diesel - gdf.sum().diesel
CO2_aequivalent_kg = Dieselersparnis * 2.6
