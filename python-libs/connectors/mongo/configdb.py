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

    def default_collection_s3(self):
        return 'uploads_s3'

    def default_collection_jobs(self):
        return 'jobs_emr'

    def default_parameters_config(self):
        return 'parameters'

    def save_jobs_upload_S3(self, **kargs):
        self.save( data=kargs,collection_name=self.default_collection_s3() )
        
    def save_jobs_upload_EMR(self, **kargs):
        self.save( data=kargs,collection_name=self.default_collection_jobs() )

    def update_jobs_scripts_s3(self, script_name, path_s3_name):
       match_criteria = {'pathS3Name':path_s3_name}
       updated = {'scriptsReader':script_name}
       self.push( match_criteria=match_criteria, 
                  values=updated,
                  collection_name = self.default_collection_s3(),
                  upsert = False)

    def get_config(self, key_name):
        return self.find({'key' : key_name},
                         collection_name=self.default_parameters_config())