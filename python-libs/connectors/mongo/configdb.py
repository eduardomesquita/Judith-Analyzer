from pymongo import MongoClient
import json, pymongo
from mongojudith import *

class ConfigDB( MongoJudithAbstract ):

    def __init__(self, ):
        MongoJudithAbstract.__init__( self, db='judith-project-config')

    def collection_name(self):
        raise NotImplementedError()

    def collection_mapper(self):
        return 'scriptsMapper'

    def collection_s3(self):
        return 'uploadAwsS3'

    def collection_jobs_emr(self):
        return 'jobsEmr'

    def collection_parameters_config(self):
        return 'parameters'

    def collection_dust_emr(self):
        return 'dustEmr'

    def collection_config_emr(self):
        return 'jobsEmrConfig'

    def collection_sequence(self):
        return 'jobsEmrSequence'

    def collection_blacklist_script(self):
        return 'scriptsBlacklist'

    def collection_log(self):
        return 'logsjobs'

    def save_log(self, **kargs):
        self.save( data=kargs,collection_name=self.collection_log() )

    def find_log(self, limit=20):
        return self.find_limit( { '$query' : {},  '$orderby': { '_id' : -1 } } ,
                        collection_name=self.collection_log(), limit=limit)


    def save_jobs_upload_S3(self, **kargs):
        self.save( data=kargs,collection_name=self.collection_s3() )
        
    def save_jobs_upload_EMR(self, **kargs):
        self.save( data=kargs,collection_name=self.collection_jobs_emr() )

    def save_dust_emr(self, **kargs ):
       kargs['DUST'] = True
       self.save( data=kargs,collection_name=self.collection_dust_emr() )

    def update_jobs_scripts_s3(self, script_name, path_s3_name):
       match_criteria = {'pathS3Name':path_s3_name}
       updated = {'scriptsReader':script_name}
       self.push( match_criteria=match_criteria, 
                  values=updated,
                  collection_name = self.collection_s3(),
                  upsert = False)

    def get_parameters(self, key_name):
        return self.find({'key' : key_name},
                         collection_name=self.collection_parameters_config())

    def is_dust_analyzed(self):
        return self.find({'DUST': True}, 
                         collection_name=self.collection_dust_emr())

    def update_dust_analyzer(self):
        match_criteria = {'DUST': True}
        updated = {'DUST': False}
        self.update( match_criteria=match_criteria, 
                     values=updated,collection_name=self.collection_dust_emr(),
                     upsert = False,
                     multi=True)


    def update_config_emr(self, **match_criteria):
      for delete in ['x', 'y']:
        if match_criteria.has_key(delete):
          del match_criteria[delete]

      self.update( match_criteria={}, 
                   values=match_criteria,
                   collection_name=self.collection_config_emr(),
                   upsert = True)


    def get_config_emr(self):
      return self.find({}, self.collection_config_emr())


    def get_all_path_aws_script_blacklist(self):
      return self.find_projection({},{'_id':0},collection_name = self.collection_blacklist_script())

    def update_aws_script_blacklist(self, name, **values):
      self.update( match_criteria={'name':name}, 
                   values=values,
                   collection_name=self.collection_blacklist_script(),
                   upsert = False)


    def get_path_aws_script_mapper(self, name):
      return self.find( match_criteria = {'name': name}, 
                        collection_name = self.collection_mapper())

    def get_all_path_aws_script_mapper(self):
      return self.find_projection({},{'_id':0},collection_name = self.collection_mapper())

    def update_aws_script_mapper(self, name, **values):
      self.update( match_criteria={'name':name}, 
                   values=values,
                   collection_name=self.collection_mapper(),
                   upsert = False)


    def get_path_aws_input_file(self, name, script_name):
        return self.find( match_criteria = {'name': name, 'scriptsReader':{'$nin' : [script_name]}}, 
                          collection_name = self.collection_s3())


    def get_jobs_emr(self):
      projection = {'_id':0,'scriptName':0, 
                   'inputFile':0,'logFile':0,'outputFile':0}
      return self.find_projection({},
                                  projection=projection,
                                  collection_name=self.collection_jobs_emr())

    def get_jobs_emr_by_name(self, name):
      match_criteria = {'name':name}
      return self.find(match_criteria,collection_name=self.collection_jobs_emr())






    def get_sequence(self, name):
      return self.find({'name':name}, collection_name=self.collection_sequence())

    
    def update_sequence(self, name, sequence):
      match_criteria = {'name':name}
      updated = {'sequence': (sequence+1)}
      self.update( match_criteria=match_criteria, values=updated,
                   collection_name=self.collection_sequence(), upsert = False)