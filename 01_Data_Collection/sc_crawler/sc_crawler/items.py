import scrapy

class ScCrawlerItem(scrapy.Item):
    """
    Modèle de données pour les publications scientifiques
    """
    
    # =========================================
    # SECTION 1: IDENTIFIANTS & MÉTADONNÉES
    # =========================================
    
    title = scrapy.Field()
    """Titre de la publication (obligatoire)"""
    
    doi = scrapy.Field()
    """Digital Object Identifier - Identifiant unique permanent
    Exemple: 10.1109/ACCESS.2023.12345 ou 10.48550/arXiv.2401.12345
    """
    
    url = scrapy.Field()
    """URL complète de l'article"""
    
    source = scrapy.Field()
    """Source de la publication
    Valeurs possibles: 'arXiv', 'IEEE', 'ACM', 'ScienceDirect', 'PubMed'
    """
    
    # =========================================
    # SECTION 2: CONTENU SCIENTIFIQUE
    # =========================================
    
    abstract = scrapy.Field()
    """Résumé de la publication"""
    
    keyword = scrapy.Field()
    """Mot-clé de recherche utilisé pour trouver l'article
    Exemple: 'Blockchain', 'Deep Learning', 'Big Data'
    """
    
    keywords = scrapy.Field()
    """Liste des mots-clés de l'article (si disponibles)
    Type: list[str]
    """
    
    # =========================================
    # SECTION 3: AUTEURS & AFFILIATIONS
    # =========================================
    
    authors = scrapy.Field()
    """Liste des auteurs
    Type: list[str]
    Exemple: ["John Doe", "Jane Smith", "Alice Johnson"]
    """
    
    affiliation = scrapy.Field()
    """Affiliation principale (nom du laboratoire/université)
    Exemple: "MIT Computer Science Lab"
    """
    
    institution = scrapy.Field()
    """Nom de l'institution/université
    Exemple: "Massachusetts Institute of Technology"
    """
    
    # =========================================
    # SECTION 4: PUBLICATION & IMPACT
    # =========================================
    
    year = scrapy.Field()
    """Année de publication
    Type: int
    Exemple: 2024
    """
    
    journal = scrapy.Field()
    """Nom du journal ou de la conférence
    Exemple: "IEEE Transactions on Neural Networks"
    """
    
    issn = scrapy.Field()
    """International Standard Serial Number
    Identifiant unique du journal
    Exemple: "2169-3536"
    """
    
    quartile = scrapy.Field()
    """Quartile du journal (classement de qualité)
    Valeurs: 'Q1', 'Q2', 'Q3', 'Q4', ou None
    Q1 = Top 25% des journaux (meilleur)
    """
    
    citations = scrapy.Field()
    """Nombre de citations
    Type: int
    Default: 0
    """
    
    impact_factor = scrapy.Field()
    """Facteur d'impact du journal (si disponible)
    Type: float
    """
    
    # =========================================
    # SECTION 5: GÉOLOCALISATION (Pour BI)
    # =========================================
    
    country = scrapy.Field()
    """Pays de l'affiliation principale
    Exemple: "USA", "France", "China"
    """
    
    city = scrapy.Field()
    """Ville de l'affiliation
    Exemple: "Paris", "New York", "Tokyo"
    """
    
    latitude = scrapy.Field()
    """Latitude pour la carte
    Type: float
    Exemple: 48.8566
    """
    
    longitude = scrapy.Field()
    """Longitude pour la carte
    Type: float
    Exemple: 2.3522
    """
    
    # =========================================
    # SECTION 6: MÉTADONNÉES TECHNIQUES
    # =========================================
    
    scraped_at = scrapy.Field()
    """Date/heure du scraping
    Type: datetime
    """
    
    spider_name = scrapy.Field()
    """Nom du spider ayant collecté l'article
    Exemple: 'arxiv', 'iee', 'pubmed'
    """
    
    # =========================================
    # SECTION 7: DONNÉES ENRICHIES (Optionnel)
    # =========================================
    
    pdf_url = scrapy.Field()
    """URL du PDF complet (si disponible)"""
    
    references = scrapy.Field()
    """Liste des références citées
    Type: list[str]
    """
    
    figures_count = scrapy.Field()
    """Nombre de figures dans l'article
    Type: int
    """
    
    tables_count = scrapy.Field()
    """Nombre de tableaux
    Type: int
    """
    
    pages = scrapy.Field()
    """Nombre de pages
    Type: int
    """
    
    language = scrapy.Field()
    """Langue de publication
    Exemple: 'en', 'fr', 'zh'
    Default: 'en'
    """
    
    open_access = scrapy.Field()
    """Indique si l'article est en accès libre
    Type: bool
    """

    # =========================================
    # MÉTHODE D'AIDE
    # =========================================
    
    def __repr__(self):
        """Représentation lisible de l'item"""
        return f"<Article: {self.get('title', 'No title')[:50]}... ({self.get('year', 'N/A')})>"


# =========================================
# FONCTIONS UTILITAIRES
# =========================================

def validate_item(item):
    """
    Valide qu'un item contient les champs minimums requis
    
    Args:
        item: Instance de ScCrawlerItem
        
    Returns:
        tuple: (is_valid, error_message)
    """
    required_fields = ['title', 'year', 'source']
    
    for field in required_fields:
        if not item.get(field):
            return False, f"Missing required field: {field}"
    
    # Validation du format de l'année
    year = item.get('year')
    if year:
        try:
            year_int = int(year)
            if year_int < 1950 or year_int > 2030:
                return False, f"Invalid year: {year}"
        except ValueError:
            return False, f"Year must be a number, got: {year}"
    
    return True, None


def clean_item(item):
    """
    Nettoie et normalise les données d'un item
    
    Args:
        item: Instance de ScCrawlerItem
        
    Returns:
        ScCrawlerItem: Item nettoyé
    """
    # Nettoyer le titre
    if item.get('title'):
        item['title'] = item['title'].strip()
    
    # Normaliser l'année en int
    if item.get('year'):
        item['year'] = int(item['year'])
    
    # Normaliser les auteurs
    if item.get('authors') and isinstance(item['authors'], list):
        item['authors'] = [a.strip() for a in item['authors'] if a.strip()]
    
    # Normaliser le quartile
    if item.get('quartile'):
        item['quartile'] = item['quartile'].upper()
        if item['quartile'] not in ['Q1', 'Q2', 'Q3', 'Q4']:
            item['quartile'] = None
    
    # S'assurer que citations est un int
    if item.get('citations'):
        try:
            item['citations'] = int(item['citations'])
        except (ValueError, TypeError):
            item['citations'] = 0
    
    return item