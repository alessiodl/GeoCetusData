import json
import psycopg2 as pspg
import geopandas as gpd
import os
from datetime import datetime

f = open("config.json", "r")
config = json.load(f)

db_name = config['DB_NAME']
db_user = config['DB_USER']
db_pass = config['DB_PASS']
db_host = config['DB_HOST']

try:
    conn = pspg.connect(database=db_name, user=db_user, password=db_pass, host=db_host)
    # print("Connessione effettuata") 
except:
    print("Connessione fallita")

tables = ["cetacei", "tartarughe"]

for table in tables:

    # Eliminazione file esistenti
    if os.path.exists(r"data/csv/"+table+".csv"):
        os.remove(r"data/csv/"+table+".csv")

    if os.path.exists(r"data/geo/"+table+".geojson"):
        os.remove(r"data/geo/"+table+".geojson")

    # Estrazione dati da Postgres
    sql = """SELECT codice, data_rilievo, regione, provincia, comune, 
                    specie, cod_specie, sesso, lunghezza AS lunghezza_cm, tipo_lunghezza, condizioni,
                    targhetta AS tag, targhetta_rilascio AS tag_rilascio, interazione,
                    segnalatore AS segnalato_da, rilevatore, struttura_rilevatore, the_geom AS geometry
            FROM """+table+"""; """

    gdf = gpd.GeoDataFrame.from_postgis(sql, conn, geom_col="geometry").sort_values(by="data_rilievo", ascending=False)
    gdf['data_rilievo'] = gdf['data_rilievo'].astype('str')

    # Scrittura file GeoJSON
    gdf.to_file(os.path.join(r'data/geo', table+".geojson"), driver='GeoJSON')

    # Scrittura file CSV
    gdf['lng'] = gdf['geometry'].x
    gdf['lat'] = gdf['geometry'].y
    df = gdf.drop('geometry', axis = 1)
    df.to_csv(os.path.join(r'data/csv', table+".csv"), encoding='utf-8', index=False)

    # Log
    '''
    with open("log.txt", "a") as log:
        log.write("Dataset {} aggiornato con successo il {}\n".format(table, datetime.now()))
        log.close()
    '''

