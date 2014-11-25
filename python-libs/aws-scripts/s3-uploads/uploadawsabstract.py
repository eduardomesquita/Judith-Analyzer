import sys, json, datetime, os, time
from datetime import datetime

current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(current_dir + '/python-libs/connectors/mongo/')
sys.path.append(current_dir + '/python-libs/aws-api/')
sys.path.append(current_dir + '/python-libs/utils/')

from twitterdb import TwitterDB
from configdb import ConfigDB
from analyzerdb import AnalyzerDB
from s3lib import S3Connector
import dateutils as date_utils
import encodingutils as econding_utils


class AwsUploadsS3Abstract(object):

    def __init__(self, folder_name_save):
        setattr(self, 'twitter_db', TwitterDB())
        setattr(self, 'config_db', ConfigDB())
        setattr(self, 'analyzer_db', AnalyzerDB())

        setattr(self, 'collections_names', ['twittersUsers', 'twittersTags'])
        bucket_name = self.__get_params__('aws_bucket_name')
        setattr(self, 'bucket_name', bucket_name)
        current_time = date_utils.current_time().replace(' ', '-')\
                                                .replace(':', '-')

        setattr(self, 'current_time', current_time )
        setattr(self, 'start', date_utils.current_time())
        setattr(self, 'count', 0 )
        setattr(self, 'folder_name_save', folder_name_save)


    def update_document_tweet(self, id_str, collection_name):
        raise NotImplementedError()
    def create_file(self, collection_name):
        raise NotImplementedError()
    def run(self):
        raise NotImplementedError()
      

    def __get_params__(self, key):
        params = list( self.config_db.get_parameters( key))
        return params[0]['value']
    
    def get_folder_name(self):
        path_folder = self.__get_params__( 'path_aws_uploads' )
        folder_name = self.folder_name_save+'/'+self.current_time
        os.mkdir(path_folder+'/'+folder_name+'/') 
        return path_folder+folder_name+'/'

    def write_file(self, path_file, content):
        arq = open(path_file, 'a')
        arq.write( content + '\n' )
        arq.close()

    def to_string(self, tweet_json):
      data = ''
      for key, values in tweet_json.iteritems():
        if isinstance(values, dict ):
          for k2, v2 in values.iteritems():
            data +=  "%s.%s=%s;" % (key, k2, v2)
        else:
            data +=  "%s=%s;" % (key, values)
      return data

    def save_s3_jobs_upload(self, file_name, s3_path_name, name):
        data =  { 'pathS3Name' : s3_path_name,
                  'counttweet' : self.count,
                  'create_at': self.current_time,
                  'runTimeMminutes' : date_utils.diff_data_minute(self.start), 
                  'name' : name,
                  'scriptsReader':[]
                }
        self.config_db.save_jobs_upload_S3( **data )