import scrapy
import re
import random
from ..items import ScCrawlerItem  # Import adapté à ton projet

class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CLOSESPIDER_ITEMCOUNT': 1000, # S'arrête après 1000 articles
        'DOWNLOAD_DELAY': 2.0,         # Pause de 2s pour ne pas se faire bannir
        'USER_AGENT': 'Mozilla/5.0 (compatible; StudentProject/1.0;)'
    }

    # Base de données fictive pour la visualisation BI
    LOCATIONS = [
        {"country": "USA", "city": "New York", "lat": 40.7128, "lon": -74.0060, "aff": "NYU"},
        {"country": "USA", "city": "San Francisco", "lat": 37.7749, "lon": -122.4194, "aff": "Stanford University"},
        {"country": "France", "city": "Paris", "lat": 48.8566, "lon": 2.3522, "aff": "Sorbonne Université"},
        {"country": "France", "city": "Lyon", "lat": 45.7640, "lon": 4.8357, "aff": "INSA Lyon"},
        {"country": "UK", "city": "London", "lat": 51.5074, "lon": -0.1278, "aff": "Imperial College"},
        {"country": "China", "city": "Beijing", "lat": 39.9042, "lon": 116.4074, "aff": "Tsinghua University"},
        {"country": "Morocco", "city": "Casablanca", "lat": 33.5731, "lon": -7.5898, "aff": "Hassan II University"},
        {"country": "Germany", "city": "Berlin", "lat": 52.5200, "lon": 13.4050, "aff": "TU Berlin"},
        {"country": "India", "city": "Bangalore", "lat": 12.9716, "lon": 77.5946, "aff": "IISc Bangalore"}
    ]

    def start_requests(self):
        keywords = ['Blockchain', 'Deep Learning', 'Big Data']
        base_url = "https://arxiv.org/search/?query={}&searchtype=all&source=header&start={}"

        for kw in keywords:
            # On lance la page 0 pour chaque mot clé
            yield scrapy.Request(
                url=base_url.format(kw, 0), 
                callback=self.parse, 
                meta={'keyword': kw, 'offset': 0}
            )

    def parse(self, response):
        results = response.css('li.arxiv-result')
        keyword = response.meta['keyword']
        offset = response.meta['offset']
        
        # On arrête si on a trop d'articles pour ce mot clé (environ 350 par mot clé pour arriver à 1000 total)
        if offset > 350:
            return

        for result in results:
            item = ScCrawlerItem()
            
            # 1. Extraction du titre
            raw_title = result.css('p.title.is-5::text').get()
            item['title'] = raw_title.strip() if raw_title else None

            # 2. Extraction et filtrage de l'année (2015-2025)
            date_text = result.css('p.is-size-7::text').get()
            item['year'] = "2024" # Valeur par défaut
            
            if date_text:
                year_match = re.search(r'(19|20)\d{2}', date_text)
                if year_match:
                    year_int = int(year_match.group(0))
                    if 2015 <= year_int <= 2026:
                        item['year'] = str(year_int)
                    else:
                        continue # On ignore les vieux articles
            
            item['keyword'] = keyword
            item['source'] = 'arXiv'
            item['authors'] = result.css('p.authors a::text').getall()

            # 3. Simulation des données géographiques (Pour le Dashboard)
            loc_data = random.choice(self.LOCATIONS)
            item['country'] = loc_data['country']
            item['city'] = loc_data['city']
            item['latitude'] = loc_data['lat']
            item['longitude'] = loc_data['lon']
            item['affiliation'] = loc_data['aff'] 

            if item['title']:
                yield item
        
        # Gestion de la pagination (Arxiv affiche 50 résultats par page)
        # On relance la requête pour la page suivante
        next_offset = offset + 50
        base_url = "https://arxiv.org/search/?query={}&searchtype=all&source=header&start={}"
        yield scrapy.Request(
            url=base_url.format(keyword, next_offset),
            callback=self.parse,
            meta={'keyword': keyword, 'offset': next_offset}
        )