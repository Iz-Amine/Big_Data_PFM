import pymongo

class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGODB_DB', 'bigdata_project'),
            mongo_collection=crawler.settings.get('MONGODB_COLLECTION', 'raw_publications')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        # Optionnel : Vider la collection avant de lancer le nouveau crawl
        if spider.name == 'arxiv':
            print("--- Nettoyage de la base de données avant insertion ---")
            self.db[self.mongo_collection].delete_many({})

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.mongo_collection].insert_one(dict(item))
        return item