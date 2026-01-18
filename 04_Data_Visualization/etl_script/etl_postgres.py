import json
import psycopg2
import os

# --- CONFIGURATION ---
# Chemin vers le fichier JSON généré par Spark (Etape précédente)
JSON_SOURCE = "../../04_Data_Visualization/4a_Web_Dashboard/static/data/global_data.json"

# Connexion PostgreSQL (infos du docker-compose)
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "science_dwh",
    "user": "admin",
    "password": "admin"
}

def create_schema(cursor):
    print("--- 1. Création du Schéma en Étoile (Star Schema) ---")
    
    # Nettoyage (Drop si existe pour repartir à zéro)
    cursor.execute("DROP TABLE IF EXISTS F_Publications CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Temps CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Geographie CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Sujet CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Source CASCADE;")

    # 1. Dimension Temps
    cursor.execute("""
        CREATE TABLE D_Temps (
            id_temps SERIAL PRIMARY KEY,
            annee INTEGER UNIQUE NOT NULL
        );
    """)

    # 2. Dimension Géographie
    cursor.execute("""
        CREATE TABLE D_Geographie (
            id_geo SERIAL PRIMARY KEY,
            pays VARCHAR(100),
            ville VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            UNIQUE(pays, ville)
        );
    """)

    # 3. Dimension Sujet (Thématique)
    cursor.execute("""
        CREATE TABLE D_Sujet (
            id_sujet SERIAL PRIMARY KEY,
            mot_cle VARCHAR(100) UNIQUE NOT NULL
        );
    """)

    # 4. Dimension Source
    cursor.execute("""
        CREATE TABLE D_Source (
            id_source SERIAL PRIMARY KEY,
            nom_source VARCHAR(100) UNIQUE NOT NULL
        );
    """)

    # 5. Table de Faits (F_Publications)
    # Elle lie toutes les dimensions et contient les métriques
    cursor.execute("""
        CREATE TABLE F_Publications (
            id_pub SERIAL PRIMARY KEY,
            titre TEXT,
            id_temps INTEGER REFERENCES D_Temps(id_temps),
            id_geo INTEGER REFERENCES D_Geographie(id_geo),
            id_sujet INTEGER REFERENCES D_Sujet(id_sujet),
            id_source INTEGER REFERENCES D_Source(id_source),
            nb_publications INTEGER DEFAULT 1
        );
    """)
    print("✅ Tables créées avec succès.")

def load_data(conn, cursor):
    print("\n--- 2. Chargement des données (ETL) ---")
    
    # Lecture du fichier JSON Spark
    if not os.path.exists(JSON_SOURCE):
        print(f"❌ ERREUR: Le fichier {JSON_SOURCE} est introuvable.")
        print("   -> As-tu bien lancé l'analyse Spark avant ?")
        return

    with open(JSON_SOURCE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"   -> {len(data)} articles trouvés dans le JSON.")

    count = 0
    for article in data:
        # Récupération des valeurs (avec gestion des NULL)
        annee = article.get('year')
        pays = article.get('country', 'Inconnu')
        ville = article.get('city', 'Inconnu')
        lat = article.get('latitude', 0.0)
        lon = article.get('longitude', 0.0)
        keyword = article.get('keyword', 'Non spécifié')
        source = article.get('source', 'Autre')
        titre = article.get('title', 'Sans titre')

        # --- A. Insertion Dimensions (et récupération des ID) ---
        
        # D_Temps
        cursor.execute("INSERT INTO D_Temps (annee) VALUES (%s) ON CONFLICT (annee) DO NOTHING;", (annee,))
        cursor.execute("SELECT id_temps FROM D_Temps WHERE annee = %s;", (annee,))
        id_temps = cursor.fetchone()[0]

        # D_Geographie
        cursor.execute("""
            INSERT INTO D_Geographie (pays, ville, latitude, longitude) 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (pays, ville) DO NOTHING;
        """, (pays, ville, lat, lon))
        cursor.execute("SELECT id_geo FROM D_Geographie WHERE pays = %s AND ville = %s;", (pays, ville))
        id_geo = cursor.fetchone()[0]

        # D_Sujet
        cursor.execute("INSERT INTO D_Sujet (mot_cle) VALUES (%s) ON CONFLICT (mot_cle) DO NOTHING;", (keyword,))
        cursor.execute("SELECT id_sujet FROM D_Sujet WHERE mot_cle = %s;", (keyword,))
        id_sujet = cursor.fetchone()[0]

        # D_Source
        cursor.execute("INSERT INTO D_Source (nom_source) VALUES (%s) ON CONFLICT (nom_source) DO NOTHING;", (source,))
        cursor.execute("SELECT id_source FROM D_Source WHERE nom_source = %s;", (source,))
        id_source = cursor.fetchone()[0]

        # --- B. Insertion Fait ---
        cursor.execute("""
            INSERT INTO F_Publications (titre, id_temps, id_geo, id_sujet, id_source, nb_publications)
            VALUES (%s, %s, %s, %s, %s, 1);
        """, (titre, id_temps, id_geo, id_sujet, id_source))
        
        count += 1
        if count % 100 == 0:
            print(f"   ... {count} lignes insérées")

    conn.commit()
    print(f"\n✅ ETL Terminé ! {count} lignes chargées dans le Data Warehouse PostgreSQL.")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_schema(cursor)
        load_data(conn, cursor)
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erreur critique : {e}")

if __name__ == "__main__":
    main()
