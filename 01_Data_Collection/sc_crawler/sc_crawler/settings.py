BOT_NAME = 'sc_crawler'
SPIDER_MODULES = ['sc_crawler.spiders']
NEWSPIDER_MODULE = 'sc_crawler.spiders'

# --- Configuration Splash (Docker) ---
SPLASH_URL = 'http://localhost:8050' 
DOWNLOADER_MIDDLEWARES = {
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'scrapy_splash.SplashMiddleware': 725,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}
SPIDER_MIDDLEWARES = {
    'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
}
DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'

# --- Configuration MongoDB ---
ITEM_PIPELINES = {
   'sc_crawler.pipelines.MongoPipeline': 300,
}
MONGO_URI = 'mongodb://localhost:27017'
MONGO_DB = 'bigdata_project'
MONGO_COLLECTION = 'raw_articles'

# --- Respect des règles ---
ROBOTSTXT_OBEY = False