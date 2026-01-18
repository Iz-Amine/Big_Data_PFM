# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 spark_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, min, max, desc, year, month, explode, split, lower, regexp_replace
from pyspark.sql.types import IntegerType, FloatType
import os

# Configuration
OUTPUT_DIR = "../../../04_Data_Visualization/4a_Web_Dashboard/static/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_analysis():
    print("\n" + "="*70)
    print("🚀 ANALYSE BIG DATA AVEC SPARK")
    print("="*70)
    
    # Création session Spark
    spark = SparkSession.builder \
        .appName("BigData_Scientific_Analysis_Complete") \
        .config("spark.master", "local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # ======================================
    # 1. CHARGEMENT DES DONNÉES
    # ======================================
    print("\n📂 1. Chargement des données depuis MongoDB...")
    
    df = spark.read.format("mongodb") \
        .option("connection.uri", "mongodb://localhost:27017") \
        .option("database", "bigdata_project") \
        .option("collection", "raw_articles") \
        .load()
    
    print(f"   ✅ {df.count()} articles chargés")
    
    # ======================================
    # 2. NETTOYAGE DES DONNÉES
    # ======================================
    print("\n🧹 2. Nettoyage et transformation des données...")
    
    # Cast des types
    df_clean = df.withColumn("year", col("year").cast(IntegerType())) \
                 .withColumn("citations", col("citations").cast(IntegerType())) \
                 .filter(col("year").isNotNull()) \
                 .filter(col("country").isNotNull())
    
    print(f"   ✅ {df_clean.count()} articles valides après nettoyage")
    
    # ======================================
    # 3. ANALYSES STATISTIQUES
    # ======================================
    print("\n📊 3. Analyses statistiques globales...")
    
    # 3.1 Statistiques générales
    stats = df_clean.select(
        count("*").alias("total"),
        avg("citations").alias("avg_citations"),
        min("year").alias("min_year"),
        max("year").alias("max_year")
    ).collect()[0]
    
    print(f"\n   📈 STATISTIQUES GLOBALES:")
    print(f"      - Total publications : {stats['total']}")
    print(f"      - Moyenne citations : {stats['avg_citations']:.2f}" if stats['avg_citations'] else "N/A")
    print(f"      - Période : {stats['min_year']} - {stats['max_year']}")
    
    # ======================================
    # 4. AGRÉGATIONS PAR DIMENSION
    # ======================================
    print("\n📊 4. Agrégations par dimension...")
    
    # 4.1 Par année
    print("\n   📅 Par année...")
    year_analysis = df_clean.groupBy("year") \
        .agg(
            count("*").alias("count"),
            avg("citations").alias("avg_citations")
        ) \
        .orderBy("year")
    
    year_analysis.show()
    year_analysis.toPandas().to_json(
        f"{OUTPUT_DIR}/publications_by_year.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : publications_by_year.json")
    
    # 4.2 Par pays (Top 10)
    print("\n   🌍 Par pays...")
    country_analysis = df_clean.groupBy("country") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .limit(10)
    
    country_analysis.show()
    country_analysis.toPandas().to_json(
        f"{OUTPUT_DIR}/publications_by_country.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : publications_by_country.json")
    
    # 4.3 Par keyword (thématique)
    print("\n   🔑 Par keyword...")
    keyword_analysis = df_clean.groupBy("keyword") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count"))
    
    keyword_analysis.show()
    keyword_analysis.toPandas().to_json(
        f"{OUTPUT_DIR}/publications_by_keyword.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : publications_by_keyword.json")
    
    # 4.4 Par ville (Top 10)
    print("\n   🏙️  Par ville...")
    city_analysis = df_clean.groupBy("city", "country") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .limit(10)
    
    city_analysis.show()
    city_analysis.toPandas().to_json(
        f"{OUTPUT_DIR}/publications_by_city.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : publications_by_city.json")
    
    # ======================================
    # 5. ANALYSE DES MOTS-CLÉS DANS LES TITRES
    # ======================================
    print("\n🔤 5. Analyse des mots-clés dans les titres...")
    
    # Extraction et comptage des mots
    words_df = df_clean.select(explode(split(lower(col("title")), " ")).alias("word")) \
        .filter(col("word").rlike("^[a-z]{4,}$"))  # Mots de 4+ lettres
    
    top_words = words_df.groupBy("word") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .limit(50)
    
    top_words.show(20)
    top_words.toPandas().to_json(
        f"{OUTPUT_DIR}/top_keywords.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : top_keywords.json")
    
    # ======================================
    # 6. ANALYSE CROISÉE : PAYS × KEYWORD
    # ======================================
    print("\n🔀 6. Analyse croisée pays × keyword...")
    
    country_keyword = df_clean.groupBy("country", "keyword") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .limit(20)
    
    country_keyword.show()
    country_keyword.toPandas().to_json(
        f"{OUTPUT_DIR}/country_keyword_matrix.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : country_keyword_matrix.json")
    
    # ======================================
    # 7. ÉVOLUTION TEMPORELLE PAR THÉMATIQUE
    # ======================================
    print("\n📈 7. Évolution temporelle par thématique...")
    
    temporal_evolution = df_clean.groupBy("year", "keyword") \
        .agg(count("*").alias("count")) \
        .orderBy("year", "keyword")
    
    temporal_evolution.show(30)
    temporal_evolution.toPandas().to_json(
        f"{OUTPUT_DIR}/temporal_evolution.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : temporal_evolution.json")
    
    # ======================================
    # 8. QUARTILE DISTRIBUTION (Simulation)
    # ======================================
    print("\n📊 8. Distribution par quartile (simulée)...")
    
    # Simulation des quartiles pour dashboard BI
    from pyspark.sql.functions import when, rand
    
    df_quartile = df_clean.withColumn(
        "quartile",
        when(rand() < 0.30, "Q1")
        .when(rand() < 0.55, "Q2")
        .when(rand() < 0.75, "Q3")
        .otherwise("Q4")
    )
    
    quartile_dist = df_quartile.groupBy("quartile") \
        .agg(count("*").alias("count")) \
        .orderBy("quartile")
    
    quartile_dist.show()
    quartile_dist.toPandas().to_json(
        f"{OUTPUT_DIR}/quartiles_distribution.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : quartiles_distribution.json")
    
    # ======================================
    # 9. TOP AUTEURS
    # ======================================
    print("\n👥 9. Top 20 auteurs...")
    
    # Exploser le tableau d'auteurs pour compter par auteur
    authors_df = df_clean.select(explode(col("authors")).alias("author"))
    
    top_authors = authors_df.groupBy("author") \
        .agg(count("*").alias("publications")) \
        .orderBy(desc("publications")) \
        .limit(20)
    
    top_authors.show()
    top_authors.toPandas().to_json(
        f"{OUTPUT_DIR}/top_authors.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : top_authors.json")
    
    # ======================================
    # 10. TOP LABORATOIRES (par ville)
    # ======================================
    print("\n🏛️ 10. Top 20 laboratoires (par ville)...")
    
    # Utiliser ville comme proxy pour laboratoire
    from pyspark.sql.functions import concat, lit
    
    top_labs = df_clean.groupBy("city", "country") \
        .agg(count("*").alias("publications")) \
        .withColumn("laboratory", concat(col("city"), lit(" Research Center"))) \
        .select("laboratory", "publications") \
        .orderBy(desc("publications")) \
        .limit(20)
    
    top_labs.show()
    top_labs.toPandas().to_json(
        f"{OUTPUT_DIR}/top_laboratories.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : top_laboratories.json")
    
    # ======================================
    # 11. EXPORT DONNÉES GLOBALES (pour le dashboard)
    # ======================================
    print("\n💾 11. Export données globales pour le dashboard web...")
    
    global_export = df_clean.select(
        "title", "year", "country", "city", "keyword", "source", "authors", "abstract", "doi", "url"
    )
    
    global_export.toPandas().to_json(
        f"{OUTPUT_DIR}/global_data.json",
        orient="records"
    )
    print(f"   ✅ Sauvegardé : global_data.json")
    
    # ======================================
    # 9. RÉSUMÉ FINAL
    # ======================================
    print("\n" + "="*70)
    print("✅ ANALYSE TERMINÉE AVEC SUCCÈS")
    print("="*70)
    print(f"\n📁 Fichiers générés dans : {OUTPUT_DIR}/")
    print("   1. publications_by_year.json")
    print("   2. publications_by_country.json")
    print("   3. publications_by_keyword.json")
    print("   4. publications_by_city.json")
    print("   5. top_keywords.json")
    print("   6. country_keyword_matrix.json")
    print("   7. temporal_evolution.json")
    print("   8. quartiles_distribution.json")
    print("   9. top_authors.json")
    print("   10. top_laboratories.json")
    print("   11. global_data.json")
    print("\n🎉 Vous pouvez maintenant lancer le dashboard Flask !")
    
    spark.stop()

if __name__ == "__main__":
    run_analysis()