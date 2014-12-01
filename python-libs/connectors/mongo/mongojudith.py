from pymongo import MongoClient
import json, pymongo

class MongoJudithAbstract(object):

    def __init__(self, host = 'localhost', port = 27017 , db  = None):
       setattr(self,'client', MongoClient( host , port ) )
       setattr(self,'mongo_db', self.client[ db ])

    def default_collection_name(self):
        raise NotImplementedError()

    def save(self, data, collection_name):
       self.mongo_db[ collection_name ].save( data )

    def remove(self, match_criteria, collection_name):
        self.mongo_db[ collection_name ].remove( match_criteria )

    def update(self, match_criteria, values, collection_name, upsert = True, multi=False):
        self.mongo_db[ collection_name ].update( match_criteria, {'$set': values}, upsert = upsert, multi=multi)

    def push(self, match_criteria, values, collection_name, upsert = True):
        self.mongo_db[ collection_name ].update( match_criteria, {'$push': values}, upsert = upsert)
        
    def find(self, match_criteria, collection_name):
        return self.mongo_db[ collection_name ].find( match_criteria )

    def find_limit(self, match_criteria, collection_name ,limit):
        return self.mongo_db[ collection_name ].find( match_criteria ).limit( limit )
        

    def find_projection(self, match_criteria, projection,  collection_name):
        return self.mongo_db[ collection_name ].find( match_criteria, projection )

    def count(self, match_criteria, collection_name):
        return self.mongo_db[ collection_name ].find( match_criteria ).count()