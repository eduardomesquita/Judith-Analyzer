import sys, json
current_dir =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(current_dir + '/python-libs/utils/')
import dateutils as date_utils
import encodingutils as econding_utils
from uploadawsabstract import *

class AwsS3AllTweetUpload(AwsUploadsS3Abstract):

    def __init__(self):
        AwsUploadsS3Abstract.__init__(self, 'all_raw_data')
        setattr(self, 'file_name', 'raw_data_twitter')
        setattr(self, 'path_file', (self.get_folder_name() + self.file_name))
        setattr(self, 'collections_names', ['twittersUsers', 'twittersTags'] )
      
    def __count_tweet__(self, collection_name):
        return self.twitter_db.count({}, collection_name )

    def update_document_tweet(self, id_str, collection_name):
        self.twitter_db.update_twitter_uploads_s3( id_str = id_str,
                                                   collection_name = collection_name)
   
    def create_file(self, collection_name):

        total_count = self.__count_tweet__( collection_name = collection_name )
        limit, skip = 300000, 0

        if total_count < limit:
            limit = total_count
       
        if limit > 0:
          for i in range(0, total_count, limit):
             skip = i
             tweets = self.twitter_db.find_raw_data_users( collection_name=collection_name, 
                                                           skip=skip, 
                                                           limit=limit)
             for tweet in tweets:
                  try:
                     tweet_formatted = self.to_string(tweet)
                     tweet_formatted = econding_utils.clear_coding(txt=tweet_formatted)
                     self.write_file(path_file=self.path_file,
                                     content=tweet_formatted.encode('utf-8') )
                     self.update_document_tweet( tweet['id_str'], collection_name ) ##flag
                     self.count += 1
                  except Exception as e:
                     print 'fail: %s' % e
                     break

    def run(self):
        print 'salvando dados locais em %s' %  self.path_file
        for name in self.collections_names:
            print 'pesquisando collection name %s' % name
            self.create_file( name )
            
        file_name =  'raw_data/all_raw_data/'+self.current_time+'/'+ self.file_name
        s3 = S3Connector()
        s3.upload_file( bucket_name=self.bucket_name,
                        file_name = file_name,
                        path_file=self.path_file )
        
        print 'fazendo upload arquivo....'
        s3_path_name = 's3n://'+self.bucket_name+'/'+file_name
        self.save_s3_jobs_upload(file_name=file_name,
                                 s3_path_name=s3_path_name,
                                 name='all_data')

        print '\n\nDocumento importados: %s' % self.count
        print 'Em aproximadamente: %s minutos' % date_utils.diff_data_minute( self.start )
        print 'S3: %s ' % ( s3_path_name)
        print 'FIM'


if __name__ == '__main__':
    aws = AwsS3AllTweetUpload()
    aws.run()