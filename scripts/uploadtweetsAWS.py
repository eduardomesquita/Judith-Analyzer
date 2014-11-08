import sys, json, datetime, os
from datetime import *

path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/python-libs/connectors/mongo/')
sys.path.append(path_python_libs + '/python-libs/aws-api/')

from mongojudith import TwitterDB
from s3lib import S3Connector


class AwsS3Upload(object):

    def __init__(self):
        setattr(self, 'twitter_db', TwitterDB())
        setattr(self,'bucket_name', self.__get_aws_params__('aws_bucket_name'))
        setattr(self, 'file_name', 'raw_data_twitter')
        setattr(self, 'path_file', (self.__get_folder_name__() + self.file_name))
        setattr(self, 'collections_names', ['twittersUsers', 'twittersTags'] )

    def __get_aws_params__(self, key):
        try:
            return list( self.twitter_db.get_config(key) )[0]['value']
        except:
            raise Exception()

    def __now__(self):
        fmt = "%Y-%m-%d %H:%M:%S"
        data_atual =  datetime.now().strftime( fmt )
        return data_atual.replace(' ', '-').replace(':', '-')

    def __get_folder_name__(self):
        path_folder = self.__get_aws_params__( 'path_aws_uploads' )
        folder_name = self.__now__()
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
            self.twitter_db.update_twitter_uploads_s3(id_str = id_str,
                                                      collection_name = collection_name)
        except:
            raise Exception()

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

                   tweet_json['_id'] = str(tweet_json['_id'] )
                   json_formatted = json.dumps( tweet_json )
                   self.__write_file__( self.path_file, json_formatted )
                   self.update_document( tweet_json['id_str'], collection_name )

                except Exception as e:
                   print 'fail: %s' % e
                   exit()

    def run(self):
        for name in self.collections_names:
            print 'pesquisando collection name %s' % name
            self.__create_file__( name )
            
        s3 = S3Connector()
        s3.upload_file( bucket_name=self.bucket_name,
                        file_name = self.__now__()+'/'+ self.file_name,
                        path_file=self.path_file )
        print 'FIM'


if __name__ == '__main__':
    aws = AwsS3Upload()
    aws.run()