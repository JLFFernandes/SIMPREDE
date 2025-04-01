import psycopg2

def update_location_centroids():
    try:
        conn = psycopg2.connect("dbname=simprede_db user=postgres password=123 host=localhost")
        cur = conn.cursor()

        # Atualizar com os centroides das freguesias
        cur.execute("""
        UPDATE location l
        SET geom = cf.geom,
            latitude = ST_Y(cf.geom),
            longitude = ST_X(cf.geom)
        FROM centroide_freg cf
        WHERE l.district = cf.distrito
          AND l.municipality = cf.concelho
          AND l.parish = cf.freguesia
          AND (l.latitude IS NULL OR l.longitude IS NULL);
        """)
        print(f"Freguesias atualizadas com sucesso! ({cur.rowcount} linhas modificadas)")

        # Atualizar com os centroides dos municípios
        cur.execute("""
        UPDATE location l
        SET geom = cc.geom,
            latitude = ST_Y(cc.geom),
            longitude = ST_X(cc.geom)
        FROM centroide_concel cc
        WHERE l.district = cc.name_1
          AND l.municipality = cc.name_2
          AND (l.latitude IS NULL OR l.longitude IS NULL);
        """)
        print(f"Municípios atualizados com sucesso! ({cur.rowcount} linhas modificadas)")

        # Atualizar com os centroides dos distritos
        cur.execute("""
        UPDATE location l
        SET geom = cd.geom,
            latitude = ST_Y(cd.geom),
            longitude = ST_X(cd.geom)
        FROM centroide_distrito cd
        WHERE l.district = cd.name_1
          AND (l.latitude IS NULL OR l.longitude IS NULL);
        """)
        print(f"Distritos atualizados com sucesso! ({cur.rowcount} linhas modificadas)")

        conn.commit()
        cur.close()
        conn.close()
        print("Conexão fechada com sucesso!")

    except Exception as e:
        print(f"Erro ao atualizar os dados: {e}")

if __name__ == "__main__":
    update_location_centroids()
