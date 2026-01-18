import json
import psycopg2
import os
import random

# --- CONFIGURATION ---
# On pointe vers le même fichier JSON que le site Web, en lecture seule.
JSON_SOURCE = "../../04_Data_Visualization/4a_Web_Dashboard/static/data/global_data.json"

# Configuration de la base PostgreSQL (Docker)
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "science_dwh",
    "user": "admin",
    "password": "admin"
}

def create_schema(cursor):
    print("--- 1. Mise à jour du Schéma (Ajout des métriques de Qualité) ---")
    
    # On nettoie proprement pour recréer la structure enrichie
    # CASCADE permet de supprimer les tables même si elles sont liées, pour éviter les erreurs
    cursor.execute("DROP TABLE IF EXISTS F_Publications CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Temps CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Geographie CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Sujet CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS D_Source CASCADE;")

    # --- Création des Dimensions (Structure inchangée pour ne pas casser l'existant) ---
    cursor.execute("CREATE TABLE D_Temps (id_temps SERIAL PRIMARY KEY, annee INTEGER UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE D_Geographie (id_geo SERIAL PRIMARY KEY, pays VARCHAR(100), ville VARCHAR(100), UNIQUE(pays, ville));")
    cursor.execute("CREATE TABLE D_Sujet (id_sujet SERIAL PRIMARY KEY, mot_cle VARCHAR(100) UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE D_Source (id_source SERIAL PRIMARY KEY, nom_source VARCHAR(100) UNIQUE NOT NULL);")

    # --- Création de la Table de Faits (ENRICHIE) ---
    # On ajoute quartile, citations et impact_factor sans toucher aux colonnes existantes
    cursor.execute("""
        CREATE TABLE F_Publications (
            id_pub SERIAL PRIMARY KEY,
            titre TEXT,
            id_temps INTEGER REFERENCES D_Temps(id_temps),
            id_geo INTEGER REFERENCES D_Geographie(id_geo),
            id_sujet INTEGER REFERENCES D_Sujet(id_sujet),
            id_source INTEGER REFERENCES D_Source(id_source),
            
            -- Métriques existantes
            nb_publications INTEGER DEFAULT 1,
            
            -- NOUVELLES Métriques pour satisfaire l'exigence "Qualité" du prof
            quartile VARCHAR(2),       -- Ex: Q1, Q2
            nb_citations INTEGER,      -- Ex: 154
            impact_factor FLOAT        -- Ex: 4.2
        );
    """)
    print("✅ Schéma V2 déployé avec succès.")

def load_data(conn, cursor):
    print("\n--- 2. Chargement et Simulation des données BI ---")
    
    if not os.path.exists(JSON_SOURCE):
        print(f"❌ ERREUR CRITIQUE : Le fichier {JSON_SOURCE} est introuvable.")
        return

    with open(JSON_SOURCE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"   -> Traitement de {len(data)} articles...")

    count = 0
    for article in data:
        # 1. Récupération des données réelles (Scrapées)
        annee = article.get('year')
        pays = article.get('country', 'Inconnu')
        ville = article.get('city', 'Inconnu')
        keyword = article.get('keyword', 'Non spécifié')
        source = article.get('source', 'Autre')
        titre = article.get('title', 'Sans titre')

        # 2. Simulation des données manquantes (Pour le Dashboard BI seulement)
        # Logique : Q1 = plus de citations, Q4 = moins de citations
        quartile = random.choice(['Q1', 'Q1', 'Q2', 'Q2', 'Q3', 'Q4']) 
        if quartile == 'Q1':
            citations = random.randint(50, 500)
            impact = round(random.uniform(3.0, 10.0), 2)
        elif quartile == 'Q2':
            citations = random.randint(20, 100)
            impact = round(random.uniform(1.5, 4.0), 2)
        else:
            citations = random.randint(0, 30)
            impact = round(random.uniform(0.1, 2.0), 2)

        # 3. Insertion Dimensions (Peuplage des tables de référence)
        cursor.execute("INSERT INTO D_Temps (annee) VALUES (%s) ON CONFLICT (annee) DO NOTHING;", (annee,))
        cursor.execute("SELECT id_temps FROM D_Temps WHERE annee = %s;", (annee,))
        id_temps = cursor.fetchone()[0]

        cursor.execute("INSERT INTO D_Geographie (pays, ville) VALUES (%s, %s) ON CONFLICT (pays, ville) DO NOTHING;", (pays, ville))
        cursor.execute("SELECT id_geo FROM D_Geographie WHERE pays = %s AND ville = %s;", (pays, ville))
        id_geo = cursor.fetchone()[0]

        cursor.execute("INSERT INTO D_Sujet (mot_cle) VALUES (%s) ON CONFLICT (mot_cle) DO NOTHING;", (keyword,))
        cursor.execute("SELECT id_sujet FROM D_Sujet WHERE mot_cle = %s;", (keyword,))
        id_sujet = cursor.fetchone()[0]

        cursor.execute("INSERT INTO D_Source (nom_source) VALUES (%s) ON CONFLICT (nom_source) DO NOTHING;", (source,))
        cursor.execute("SELECT id_source FROM D_Source WHERE nom_source = %s;", (source,))
        id_source = cursor.fetchone()[0]

        # 4. Insertion Fait (Liaison + Métriques)
        cursor.execute("""
            INSERT INTO F_Publications 
            (titre, id_temps, id_geo, id_sujet, id_source, nb_publications, quartile, nb_citations, impact_factor)
            VALUES (%s, %s, %s, %s, %s, 1, %s, %s, %s);
        """, (titre, id_temps, id_geo, id_sujet, id_source, quartile, citations, impact))
        
        count += 1

    conn.commit()
    print(f"✅ ETL Terminé ! {count} lignes chargées dans PostgreSQL avec les données de qualité simulées.")

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        create_schema(cursor)
        load_data(conn, cursor)
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erreur de connexion ou SQL : {e}")

if __name__ == "__main__":
    main()
