
import scrapy
import re
import random
from ..items import ScCrawlerItem


class ArxivSpider(scrapy.Spider):
    name = "arxiv"
    allowed_domains = ["arxiv.org"]
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CLOSESPIDER_ITEMCOUNT': 1000,
        'DOWNLOAD_DELAY': 2.0,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    # Base de données fictive pour la visualisation BI (à enrichir)
    LOCATIONS = [
        {"country": "USA", "city": "New York", "lat": 40.7128, "lon": -74.0060, "aff": "New York University", "institution": "NYU"},
        {"country": "USA", "city": "San Francisco", "lat": 37.7749, "lon": -122.4194, "aff": "Stanford University", "institution": "Stanford"},
        {"country": "USA", "city": "Cambridge", "lat": 42.3736, "lon": -71.1097, "aff": "MIT", "institution": "Massachusetts Institute of Technology"},
        {"country": "France", "city": "Paris", "lat": 48.8566, "lon": 2.3522, "aff": "Sorbonne Université", "institution": "Sorbonne"},
        {"country": "France", "city": "Lyon", "lat": 45.7640, "lon": 4.8357, "aff": "INSA Lyon", "institution": "INSA"},
        {"country": "UK", "city": "London", "lat": 51.5074, "lon": -0.1278, "aff": "Imperial College London", "institution": "Imperial"},
        {"country": "UK", "city": "Oxford", "lat": 51.7520, "lon": -1.2577, "aff": "University of Oxford", "institution": "Oxford"},
        {"country": "China", "city": "Beijing", "lat": 39.9042, "lon": 116.4074, "aff": "Tsinghua University", "institution": "Tsinghua"},
        {"country": "China", "city": "Shanghai", "lat": 31.2304, "lon": 121.4737, "aff": "Fudan University", "institution": "Fudan"},
        {"country": "Morocco", "city": "Casablanca", "lat": 33.5731, "lon": -7.5898, "aff": "Hassan II University", "institution": "Hassan II"},
        {"country": "Morocco", "city": "Rabat", "lat": 34.0209, "lon": -6.8416, "aff": "Mohammed V University", "institution": "Mohammed V"},
        {"country": "Germany", "city": "Berlin", "lat": 52.5200, "lon": 13.4050, "aff": "TU Berlin", "institution": "Technical University of Berlin"},
        {"country": "Germany", "city": "Munich", "lat": 48.1351, "lon": 11.5820, "aff": "TU Munich", "institution": "Technical University of Munich"},
        {"country": "India", "city": "Bangalore", "lat": 12.9716, "lon": 77.5946, "aff": "IISc Bangalore", "institution": "Indian Institute of Science"},
        {"country": "Canada", "city": "Toronto", "lat": 43.6532, "lon": -79.3832, "aff": "University of Toronto", "institution": "Toronto"},
        {"country": "Japan", "city": "Tokyo", "lat": 35.6762, "lon": 139.6503, "aff": "University of Tokyo", "institution": "Tokyo"},
    ]

    def start_requests(self):
        """
        Point d'entrée du spider
        """
        keywords = ['Blockchain', 'Deep Learning', 'Big Data']
        base_url = "https://arxiv.org/search/?query={}&searchtype=all&source=header&start={}"

        for kw in keywords:
            self.logger.info(f"🔍 Starting scraping for keyword: {kw}")
            yield scrapy.Request(
                url=base_url.format(kw, 0), 
                callback=self.parse, 
                meta={'keyword': kw, 'offset': 0},
                dont_filter=True
            )

    def parse(self, response):
        """
        Parser principal pour chaque page de résultats
        """
        results = response.css('li.arxiv-result')
        keyword = response.meta['keyword']
        offset = response.meta['offset']
        
        self.logger.info(f"📄 Processing page at offset {offset} for '{keyword}' - Found {len(results)} results")
        
        # Limite par keyword (~333 articles chacun pour atteindre 1000 total)
        if offset > 350:
            self.logger.info(f"⛔ Reached limit for keyword '{keyword}' at offset {offset}")
            return

        # Compteur pour cette page
        valid_items = 0
        skipped_items = 0

        for result in results:
            item = ScCrawlerItem()
            
            # ========================================
            # 1. EXTRACTION DU TITRE (Obligatoire)
            # ========================================
            raw_title = result.css('p.title.is-5::text').get()
            if not raw_title:
                self.logger.warning(f"⚠️  Skipped: No title found")
                skipped_items += 1
                continue
            
            item['title'] = raw_title.strip()

            # ========================================
            # 2. EXTRACTION DE L'ANNÉE (Avec filtre)
            # ========================================
            date_text = result.css('p.is-size-7::text').get()
            item['year'] = 2024  # Valeur par défaut
            
            if date_text:
                year_match = re.search(r'(19|20)\d{2}', date_text)
                if year_match:
                    year_int = int(year_match.group(0))
                    if 2015 <= year_int <= 2026:
                        item['year'] = year_int
                    else:
                        self.logger.debug(f"⏭️  Skipped old article ({year_int}): {item['title'][:50]}")
                        skipped_items += 1
                        continue

            # ========================================
            # 3. EXTRACTION DES AUTEURS
            # ========================================
            authors_list = result.css('p.authors a::text').getall()
            item['authors'] = [author.strip() for author in authors_list if author.strip()]
            
            if not item['authors']:
                item['authors'] = ["Unknown"]

            # ========================================
            # 4. EXTRACTION DE L'ABSTRACT (CORRECTION MAJEURE)
            # ========================================
            # Deux stratégies:
            # - Texte court visible
            abstract_short = result.css('span.abstract-short::text').getall()
            # - Texte complet (caché mais présent en HTML)
            abstract_full = result.css('span.abstract-full::text').getall()
            
            if abstract_full:
                item['abstract'] = ' '.join(abstract_full).strip()
            elif abstract_short:
                item['abstract'] = ' '.join(abstract_short).strip()
            else:
                item['abstract'] = "No abstract available"

            # ========================================
            # 5. EXTRACTION DU DOI / ArXiv ID
            # ========================================
            # arXiv utilise des IDs comme "2401.12345"
            arxiv_link = result.css('p.list-title a::attr(href)').get()
            if arxiv_link:
                # Extraire l'ID arXiv depuis l'URL
                arxiv_id_match = re.search(r'(\d{4}\.\d{4,5})', arxiv_link)
                if arxiv_id_match:
                    arxiv_id = arxiv_id_match.group(1)
                    item['doi'] = f"10.48550/arXiv.{arxiv_id}"
                    item['url'] = f"https://arxiv.org/abs/{arxiv_id}"
                else:
                    item['doi'] = None
                    item['url'] = arxiv_link if arxiv_link.startswith('http') else f"https://arxiv.org{arxiv_link}"
            else:
                item['doi'] = None
                item['url'] = None

            # ========================================
            # 6. MÉTADONNÉES FIXES
            # ========================================
            item['keyword'] = keyword
            item['source'] = 'arXiv'
            item['journal'] = 'arXiv preprint'  # arXiv n'est pas un journal peer-reviewed
            item['quartile'] = None  # arXiv n'a pas de quartile
            item['citations'] = 0  # Nécessiterait une API externe (Google Scholar, Semantic Scholar)

            # ========================================
            # 7. SIMULATION DES DONNÉES GÉOGRAPHIQUES
            # ========================================
            loc_data = random.choice(self.LOCATIONS)
            item['country'] = loc_data['country']
            item['city'] = loc_data['city']
            item['latitude'] = loc_data['lat']
            item['longitude'] = loc_data['lon']
            item['affiliation'] = loc_data['aff']
            item['institution'] = loc_data['institution']

            # ========================================
            # 8. VALIDATION FINALE
            # ========================================
            if item['title'] and item['year']:
                valid_items += 1
                self.logger.info(f"✅ [{valid_items}] Scraped: {item['title'][:60]}... ({item['year']})")
                yield item
            else:
                skipped_items += 1
                self.logger.warning(f"❌ Skipped incomplete item")
        
        # ========================================
        # PAGINATION
        # ========================================
        self.logger.info(f"📊 Page summary: {valid_items} valid, {skipped_items} skipped")
        
        if valid_items > 0:  # Continue seulement si on a trouvé des articles valides
            next_offset = offset + 50
            base_url = "https://arxiv.org/search/?query={}&searchtype=all&source=header&start={}"
            
            self.logger.info(f"➡️  Moving to next page (offset {next_offset})")
            
            yield scrapy.Request(
                url=base_url.format(keyword, next_offset),
                callback=self.parse,
                meta={'keyword': keyword, 'offset': next_offset},
                dont_filter=True
            )
        else:
            self.logger.warning(f"⚠️  No valid items on this page, stopping pagination for '{keyword}'")


    def closed(self, reason):
        """
        Appelé à la fin du spider
        """
        self.logger.info(f"🏁 Spider closed: {reason}")
        self.logger.info(f"📈 Check your MongoDB to verify the scraped data")