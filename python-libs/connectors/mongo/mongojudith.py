from pymongo import MongoClient
import json, pymongo

class MongoJudithAbstract(object):

    def __init__(self, host = 'localhost', port = 27017 , db  = None):
       setattr(self,'client', MongoClient( host , port ) )
       setattr(self,'mongo_db', self.client[ db ])

    def default_collection_name(self):
        raise NotImplementedError()

    def default_db_config(self):
        return 'judith-project-config'

    def default_parameters_config(self):
        return 'parameters'



    def save(self, json_save, collection_name, db = None):
        if not db:
            self.mongo_db[ collection_name ].save( json_save )
        else:
            mongo_db  = self.client[ db ]
            mongo_db[ collection_name ].save( json_save )

    def update(self, key, values, collection_name, upsert = True):
        self.mongo_db[ collection_name ].update( key, {'$set': values}, upsert = upsert)

    def find(self, json_find, collection_name):
        return self.mongo_db[ collection_name ].find( json_find)

    def count(self, json_find, collection_name):
        return self.mongo_db[ collection_name ].find( json_find ).count()