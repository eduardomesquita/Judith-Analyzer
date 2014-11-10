from pymongo import MongoClient
import json, pymongo
from mongojudith import *

class ConfigDB( MongoJudithAbstract ):

    def __init__(self, ):
        MongoJudithAbstract.__init__( self, db='judith-project-config')

    def default_collection_name(self):
        raise NotImplementedError()

    def default_collection_mapper(self):
        return 'scriptsMapper'

    def save_jobs_upload_S3(self, path_s3_name, count, date, upload_time):
        db_config = self.default_db_config()
        
        json_save = { 'path_s3_name' : path_s3_name,
                      'count_tweet' : count,
                      'create_at': date,
                      'upload_time' :upload_time, 
                      'name' : 'all_data' }

        self.save( json_save = json_save, collection_name = 'uploads_s3',
                   db=db_config )

    def get_config(self, key_name):
        db =  self.client[ self.default_db_config() ]
        return db[ self.default_parameters_config() ].find({'key' : key_name})