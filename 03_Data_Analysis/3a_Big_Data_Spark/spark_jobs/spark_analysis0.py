from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, explode, split
import os

# Configuration des sorties
OUTPUT_DIR = "../../../04_Data_Visualization/4a_Web_Dashboard/static/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_analysis():
    spark = SparkSession.builder \
        .appName("BigData_Scientific_Analysis") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    # Chargement
    df = spark.read.format("mongodb") \
        .option("connection.uri", "mongodb://localhost:27017") \
        .option("database", "bigdata_project") \
        .option("collection", "raw_publications") \
        .load()

    # Nettoyage
    if "year" in df.columns:
        df_clean = df.withColumn("year", col("year").cast("integer"))
    else:
        df_clean = df.withColumn("year", col("date_pub").cast("integer"))

    # --- NOUVEAU : Export Global pour le Dashboard Interactif ---
    # On sélectionne les colonnes nécessaires pour le filtrage côté Web
    # Cela permet au Dashboard de filtrer "Blockchain" vs "Deep Learning" sans recharger Spark
    print("\n--- Exportation des données globales pour interactivité ---")
    
    global_export = df_clean.select("title", "year", "country", "city", "keyword", "source") \
                            .filter(col("year").isNotNull()) \
                            .filter(col("country").isNotNull())
    
    # On sauvegarde tout dans un seul fichier JSON optimisé
    global_export.toPandas().to_json(f"{OUTPUT_DIR}/global_data.json", orient="records")

    print(f"\n✅ Succès : 'global_data.json' généré dans {OUTPUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    run_analysis()