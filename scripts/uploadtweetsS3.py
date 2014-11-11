import sys, json, datetime, os, time
from datetime import datetime

path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/python-libs/connectors/mongo/')
sys.path.append(path_python_libs + '/python-libs/aws-api/')
sys.path.append(path_python_libs + '/python-libs/utils/')

from twitterdb import TwitterDB
from configdb import ConfigDB
from s3lib import S3Connector
import dateutils as date_utils
import encodingutils as econding_utils

class AwsS3Upload(object):

    def __init__(self):
        setattr(self, 'twitter_db', TwitterDB())
        setattr(self, 'config_db', ConfigDB())
        setattr(self, 'current_time', date_utils.current_time().replace(' ', '-').replace(':', '-'))
        setattr(self,'bucket_name', self.__get_aws_params__('aws_bucket_name'))
        setattr(self, 'file_name', 'raw_data_twitter')
        setattr(self, 'path_file', (self.__get_folder_name__() + self.file_name))
        setattr(self, 'collections_names', ['twittersUsers', 'twittersTags'] )
        setattr(self, 'count', 0 )
        setattr(self, 'start', date_utils.current_time())
       

    def __get_aws_params__(self, key):
        try:
            return list( self.config_db.get_config(key) )[0]['value']
        except:
            raise Exception()
            
    def __save_s3_upload__(self, file_name, s3_path_name):

        data = { 'pathS3Name' : s3_path_name,
                 'counttweet' : self.count,
                 'create_at': self.current_time,
                 'runTimeMminutes' : date_utils.diff_data_minute(self.start), 
                 'name' : 'all_data',
                 'scriptsReader':[] }

        self.config_db.save_jobs_upload_S3( **data )

    def __get_folder_name__(self):
        path_folder = self.__get_aws_params__( 'path_aws_uploads' )
        folder_name = self.current_time
        os.mkdir( path_folder +  folder_name + '/') 
        return path_folder+folder_name+'/'

    def __count_tweet__(self, collection_name):
        return self.twitter_db.count({}, collection_name )

    def __write_file__(self, path_file, content):
        arq = open(path_file, 'a')
        arq.write( content + '\n' )
        arq.close()
    
    def update_document(self, id_str, collection_name):
        try:
            self.twitter_db.update_twitter_uploads_s3( id_str = id_str,
                                                      collection_name = collection_name)
        except:
            raise Exception()

    def to_string(self, tweet_json):
      data = ''
      for key, values in tweet_json.iteritems():
        if isinstance(values, dict ):
          for k2, v2 in values.iteritems():
            data +=  "%s.%s=%s;" % (key, k2, v2)
        else:
            data +=  "%s=%s;" % (key, values)
      return data

    def __create_file__(self, collection_name):

        total_count = self.__count_tweet__( collection_name = collection_name )
        limit, skip = 7, 0

        for i in range(0, total_count, limit):
           skip = i
           tweets = self.twitter_db.find_raw_data_users( collection_name = collection_name, 
                                                         skip = skip, 
                                                         limit = limit )
           for tweet_json in tweets:
                try:

                   json_formatted = self.to_string( tweet_json )
                   json_formatted = econding_utils.clear_coding( json_formatted )
                   
                   self.__write_file__( self.path_file, json_formatted.encode('utf-8') )

                   self.update_document( tweet_json['id_str'], collection_name )
                   self.count += 1

                except Exception as e:
                   print 'fail: %s' % e
                   exit()

    def run(self):
        for name in self.collections_names:
            print 'pesquisando collection name %s' % name
            self.__create_file__( name )
            
        file_name =  'raw_data/'+self.current_time+'/'+ self.file_name
        s3 = S3Connector()

        s3.upload_file( bucket_name=self.bucket_name,
                        file_name = file_name,
                        path_file=self.path_file )
      
        s3_path_name = 's3n://'+self.bucket_name+'/'+file_name
        self.__save_s3_upload__(file_name, s3_path_name)

        print '\n\nDocumento importados: %s' % self.count
        print 'Em aproximadamente: %s minutos' % date_utils.diff_data_minute( self.start )
        print 'S3: %s ' % ( s3_path_name)
        print 'FIM'


if __name__ == '__main__':
    aws = AwsS3Upload()
    aws.run()