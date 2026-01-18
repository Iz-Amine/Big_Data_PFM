"""
Pipelines.py - Pipeline MongoDB Amélioré
=========================================

Améliorations:
1. ✅ Validation des données avant insertion
2. ✅ Nettoyage et normalisation
3. ✅ Dédoublonnage intelligent
4. ✅ Statistiques de scraping
5. ✅ Gestion d'erreurs robuste
6. ✅ Logs détaillés

Auteur: Correction Claude
"""

import pymongo
from scrapy.exceptions import DropItem
from datetime import datetime
import logging


class MongoPipeline(object):
    """
    Pipeline principal pour stocker les articles dans MongoDB
    """
    
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        
        # Compteurs de statistiques
        self.items_inserted = 0
        self.items_duplicates = 0
        self.items_invalid = 0
        
        # Logger
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Méthode appelée par Scrapy pour instancier le pipeline
        """
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB', 'bigdata_project'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION', 'raw_publications')
        )

    def open_spider(self, spider):
        """
        Exécuté au démarrage du spider
        """
        try:
            # Connexion à MongoDB
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            
            self.logger.info(f"✅ Connected to MongoDB: {self.mongo_db}.{self.mongo_collection}")
            
            # Créer des index pour optimiser les recherches
            self._create_indexes()
            
            # Optionnel : Nettoyage de la collection avant insertion
            # (Décommenter si vous voulez repartir de zéro à chaque run)
            # if spider.name == 'arxiv':
            #     self.logger.warning("⚠️  Cleaning database before insertion...")
            #     self.db[self.mongo_collection].delete_many({})
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise

    def _create_indexes(self):
        """
        Crée des index pour accélérer les requêtes
        """
        try:
            # Index sur le titre (pour dédoublonnage)
            self.db[self.mongo_collection].create_index("title")
            
            # Index sur le DOI (identifiant unique)
            self.db[self.mongo_collection].create_index("doi", sparse=True)
            
            # Index sur l'année (pour les analyses temporelles)
            self.db[self.mongo_collection].create_index("year")
            
            # Index composé pour les recherches fréquentes
            self.db[self.mongo_collection].create_index([
                ("keyword", pymongo.ASCENDING),
                ("year", pymongo.DESCENDING)
            ])
            
            self.logger.info("📑 Database indexes created successfully")
            
        except Exception as e:
            self.logger.warning(f"⚠️  Could not create indexes: {e}")

    def close_spider(self, spider):
        """
        Exécuté à la fermeture du spider
        """
        # Afficher les statistiques
        self.logger.info("=" * 60)
        self.logger.info("📊 SCRAPING STATISTICS")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ Items inserted:    {self.items_inserted}")
        self.logger.info(f"♻️  Duplicates found:  {self.items_duplicates}")
        self.logger.info(f"❌ Invalid items:     {self.items_invalid}")
        self.logger.info(f"📈 Total processed:   {self.items_inserted + self.items_duplicates + self.items_invalid}")
        self.logger.info("=" * 60)
        
        # Fermer la connexion
        self.client.close()
        self.logger.info("🔌 MongoDB connection closed")

    def process_item(self, item, spider):
        """
        Traite chaque item avant l'insertion
        
        Args:
            item: L'item à traiter
            spider: Le spider qui a généré l'item
            
        Returns:
            item: L'item traité
            
        Raises:
            DropItem: Si l'item est invalide ou en doublon
        """
        
        # =========================================
        # ÉTAPE 1: VALIDATION DE BASE
        # =========================================
        if not self._validate_item(item):
            self.items_invalid += 1
            raise DropItem(f"❌ Invalid item: {item.get('title', 'No title')[:50]}")
        
        # =========================================
        # ÉTAPE 2: NETTOYAGE DES DONNÉES
        # =========================================
        item = self._clean_item(item)
        
        # =========================================
        # ÉTAPE 3: ENRICHISSEMENT
        # =========================================
        item = self._enrich_item(item, spider)
        
        # =========================================
        # ÉTAPE 4: VÉRIFICATION DES DOUBLONS
        # =========================================
        if self._is_duplicate(item):
            self.items_duplicates += 1
            raise DropItem(f"♻️  Duplicate: {item['title'][:50]}...")
        
        # =========================================
        # ÉTAPE 5: INSERTION DANS MONGODB
        # =========================================
        try:
            self.db[self.mongo_collection].insert_one(dict(item))
            self.items_inserted += 1
            
            if self.items_inserted % 50 == 0:  # Log tous les 50 articles
                self.logger.info(f"📊 Progress: {self.items_inserted} articles inserted")
            
            return item
            
        except Exception as e:
            self.logger.error(f"❌ Failed to insert item: {e}")
            self.items_invalid += 1
            raise DropItem(f"Database insertion failed: {e}")

    def _validate_item(self, item):
        """
        Valide qu'un item contient les champs obligatoires
        
        Args:
            item: L'item à valider
            
        Returns:
            bool: True si valide, False sinon
        """
        # Champs obligatoires
        required_fields = ['title', 'source']
        
        for field in required_fields:
            if not item.get(field):
                self.logger.warning(f"⚠️  Missing required field '{field}'")
                return False
        
        # Validation du titre (minimum 5 caractères)
        if len(item.get('title', '')) < 5:
            self.logger.warning(f"⚠️  Title too short: {item.get('title')}")
            return False
        
        # Validation de l'année (si présente)
        if item.get('year'):
            try:
                year = int(item['year'])
                if year < 1950 or year > 2030:
                    self.logger.warning(f"⚠️  Invalid year: {year}")
                    return False
            except (ValueError, TypeError):
                self.logger.warning(f"⚠️  Year must be a number: {item.get('year')}")
                return False
        
        return True

    def _clean_item(self, item):
        """
        Nettoie et normalise les données
        
        Args:
            item: L'item à nettoyer
            
        Returns:
            item: L'item nettoyé
        """
        # Nettoyer le titre (enlever espaces multiples, retours à la ligne)
        if item.get('title'):
            item['title'] = ' '.join(item['title'].split())
        
        # Normaliser l'année en int
        if item.get('year'):
            item['year'] = int(item['year'])
        
        # Nettoyer l'abstract
        if item.get('abstract'):
            item['abstract'] = ' '.join(item['abstract'].split())
            # Limiter la longueur si trop long
            if len(item['abstract']) > 5000:
                item['abstract'] = item['abstract'][:5000] + "..."
        
        # Nettoyer la liste des auteurs
        if item.get('authors'):
            if isinstance(item['authors'], list):
                item['authors'] = [a.strip() for a in item['authors'] if a and a.strip()]
            else:
                item['authors'] = [str(item['authors']).strip()]
        
        # Normaliser le quartile
        if item.get('quartile'):
            item['quartile'] = str(item['quartile']).upper()
            if item['quartile'] not in ['Q1', 'Q2', 'Q3', 'Q4']:
                item['quartile'] = None
        
        # S'assurer que citations est un int
        if item.get('citations'):
            try:
                item['citations'] = int(item['citations'])
            except (ValueError, TypeError):
                item['citations'] = 0
        else:
            item['citations'] = 0
        
        # Normaliser les coordonnées géographiques
        if item.get('latitude'):
            try:
                item['latitude'] = float(item['latitude'])
            except (ValueError, TypeError):
                item['latitude'] = None
        
        if item.get('longitude'):
            try:
                item['longitude'] = float(item['longitude'])
            except (ValueError, TypeError):
                item['longitude'] = None
        
        return item

    def _enrich_item(self, item, spider):
        """
        Enrichit l'item avec des métadonnées supplémentaires
        
        Args:
            item: L'item à enrichir
            spider: Le spider source
            
        Returns:
            item: L'item enrichi
        """
        # Ajouter la date de scraping
        item['scraped_at'] = datetime.now()
        
        # Ajouter le nom du spider
        item['spider_name'] = spider.name
        
        # Ajouter un ID unique si DOI absent
        if not item.get('doi'):
            # Créer un pseudo-ID basé sur le titre et l'année
            title_hash = hash(item.get('title', ''))
            item['doi'] = f"local:{abs(title_hash)}"
        
        return item

    def _is_duplicate(self, item):
        """
        Vérifie si l'article existe déjà dans la base
        
        Args:
            item: L'item à vérifier
            
        Returns:
            bool: True si doublon, False sinon
        """
        # Stratégie 1: Vérifier par DOI (le plus fiable)
        if item.get('doi') and not item['doi'].startswith('local:'):
            existing = self.db[self.mongo_collection].find_one({'doi': item['doi']})
            if existing:
                return True
        
        # Stratégie 2: Vérifier par titre exact
        existing = self.db[self.mongo_collection].find_one({
            'title': item.get('title')
        })
        if existing:
            return True
        
        # Stratégie 3: Vérifier par similarité de titre (optionnel, plus lent)
        # Pour éviter les doublons avec variations mineures du titre
        # (décommenter si nécessaire)
        """
        similar = self.db[self.mongo_collection].find_one({
            'title': {'$regex': f"^{re.escape(item['title'][:30])}", '$options': 'i'}
        })
        if similar:
            return True
        """
        
        return False


class DataQualityPipeline(object):
    """
    Pipeline optionnel pour améliorer la qualité des données
    """
    
    def process_item(self, item, spider):
        """
        Effectue des vérifications supplémentaires de qualité
        """
        
        # Vérifier la cohérence des données géographiques
        if item.get('country') and not item.get('city'):
            spider.logger.warning(f"⚠️  Country without city: {item['title'][:50]}")
        
        # Vérifier que le nombre d'auteurs est raisonnable
        if item.get('authors') and len(item['authors']) > 50:
            spider.logger.warning(f"⚠️  Suspicious number of authors ({len(item['authors'])}): {item['title'][:50]}")
        
        # Vérifier la présence d'un abstract
        if not item.get('abstract') or len(item.get('abstract', '')) < 50:
            spider.logger.debug(f"ℹ️  Short or missing abstract: {item['title'][:50]}")
        
        return item

