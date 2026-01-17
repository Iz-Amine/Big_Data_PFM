import scrapy

class ScCrawlerItem(scrapy.Item):
    # Champs de base
    title = scrapy.Field()
    authors = scrapy.Field()
    year = scrapy.Field()
    abstract = scrapy.Field()
    source = scrapy.Field()
    keyword = scrapy.Field()
    
    # --- Nouveaux champs pour la BI (Carte) ---
    affiliation = scrapy.Field()
    country = scrapy.Field()
    city = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()