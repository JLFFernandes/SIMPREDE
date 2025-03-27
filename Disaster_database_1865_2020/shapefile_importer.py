import geopandas as gpd
import psycopg2
from shapely.geometry import Point
from datetime import datetime

def get_connection():
    """Conectar ao PostgreSQL"""
    return psycopg2.connect("dbname=simprede_db user=postgres password=123 host=localhost")

def process_date(date_string):
    """Converter uma string de data para o formato 'YYYY-MM-DD'"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except:
        return None

def import_shapefile(file_path):
    """Importar dados do Shapefile para a base de dados"""
    conn = get_connection()
    cur = conn.cursor()
    
    # Carregar o Shapefile com geopandas
    gdf = gpd.read_file(file_path)
    
    for index, row in gdf.iterrows():
        try:
            # Preparar os dados para a tabela disasters
            type = row['intype']
            subtype = row['insubtype']
            date = process_date(row['indatei'])
            year = int(row['ano']) if row['ano'] else None
            month = int(row['mês']) if row['mês'] else None
            day = int(row['dia']) if row['dia'] else None
            hour = row['inhour']

            # Inserir na tabela disasters e obter o ID gerado
            cur.execute("""
                INSERT INTO disasters (type, subtype, date, year, month, day, hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (type, subtype, date, year, month, day, hour))
            disaster_id = cur.fetchone()[0]

            # Preparar os dados para a tabela location
            district = row['Distrito']
            municipality = row['Concelho']
            parish = row['Des_Simpli']
            dicofreg = row['DICOFRE']
            latitude = row.geometry.y
            longitude = row.geometry.x
            georef_class = 1  # Considerar como localização exata

            cur.execute("""
                INSERT INTO location (id, district, municipality, parish, dicofreg, latitude, longitude, georef_class, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
            """, (disaster_id, district, municipality, parish, dicofreg, latitude, longitude, georef_class, longitude, latitude))

            # Preparar os dados para a tabela human_impacts
            fatalities = int(row['indead']) if row['indead'] else 0
            injured = int(row['ininjured']) if row['ininjured'] else 0
            evacuated = int(row['inevac']) if row['inevac'] else 0
            displaced = int(row['indisplace']) if row['indisplace'] else 0
            missing = int(row['indisap']) if row['indisap'] else 0

            cur.execute("""
                INSERT INTO human_impacts (id, fatalities, injured, evacuated, displaced, missing)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (disaster_id, fatalities, injured, evacuated, displaced, missing))

            # Preparar os dados para a tabela information_sources
            source_name = row['insource']
            source_date = process_date(row['insourcedt'])
            source_type = row['insourcety']
            page = str(row['inpg']) if row['inpg'] else None

            cur.execute("""
                INSERT INTO information_sources (disaster_id, source_name, source_date, source_type, page)
                VALUES (%s, %s, %s, %s, %s);
            """, (disaster_id, source_name, source_date, source_type, page))

            # Confirmar todas as inserções
            conn.commit()
            print(f"Dados do desastre {disaster_id} inseridos com sucesso!")

        except Exception as e:
            conn.rollback()
            print(f"Erro ao inserir o desastre {row['ID']}: {e}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    # Caminho para o teu Shapefile
    shapefile_path = '/home/pcc/projeto_desatres_ml/dados/Disaster_database_1865_2020/pts_disaster_etrs_1865_2020.shp'
    
    # Importar os dados do Shapefile
    import_shapefile(shapefile_path)
