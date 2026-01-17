import scrapy
from scrapy_splash import SplashRequest
from ..items import ScCrawlerItem

class IeeSpider(scrapy.Spider):
    name = 'iee'
    
    def start_requests(self):
        # On cible une recherche simple sur "Blockchain"
        url = 'https://ieeexplore.ieee.org/search/searchresult.jsp?newsearch=true&queryText=blockchain'
        # wait=5 pour laisser le temps au site lourd de charger
        yield SplashRequest(url, self.parse, args={'wait': 5})

    def parse(self, response):
        # DEBUG : On affiche le nombre de liens trouvés pour vérifier
        # IEEE utilise souvent la classe 'result-item-title' ou des balises h2 simples pour les titres
        articles = response.css('h2 a::attr(href)').extract()
        
        print(f"\n--- DEBUG : J'ai trouvé {len(articles)} articles potentiels ---\n")

        # Si on ne trouve rien avec le sélecteur précis, on prend tous les liens qui contiennent 'document'
        if not articles:
             articles = [x for x in response.css('a::attr(href)').extract() if 'document' in x]

        # On limite à 1 seul article pour le test
        if articles:
            first_article_link = articles[0]
            if not first_article_link.startswith('http'):
                first_article_link = 'https://ieeexplore.ieee.org' + first_article_link
            
            print(f"--- CIBLE : Je vais scraper : {first_article_link} ---")
            yield SplashRequest(first_article_link, self.parse_details, args={'wait': 5})
        else:
            print("--- ERREUR : Aucun article trouvé. Le site a peut-être bloqué ou n'a pas chargé. ---")

    def parse_details(self, response):
        item = ScCrawlerItem()
        # Sélecteurs plus larges pour attraper le titre
        item['title'] = response.css('h1::text').get() or response.css('.document-title span::text').get()
        item['authors'] = "Amine (Test)" # Simplification pour valider l'insertion Mongo
        item['date_pub'] = "2026"
        item['keyword'] = 'blockchain'
        
        yield item