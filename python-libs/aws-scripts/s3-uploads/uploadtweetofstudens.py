import sys, json
current_dir =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(current_dir + '/python-libs/utils/')
import dateutils as date_utils
import encodingutils as econding_utils
from uploadawsabstract import *

class AwsS3TweetsOfStudens(AwsUploadsS3Abstract):

    def __init__(self):
        AwsUploadsS3Abstract.__init__(self, 'raw_data_students')
        setattr(self, 'file_name', 'raw_data_students')
        setattr(self, 'path_file', (self.get_folder_name() + self.file_name))
        setattr(self, 'collections_names', ['twittersUsers', 'twittersTags'] )
        setattr(self, 'students', {} )
        self.__init__students__()


    def __init__students__(self):
        for tweet in  self.analyzer_db.get_raw_data_students():
          self.students[tweet['userName']] = tweet['statusStudents']

    def update_document_tweet(self, id_str, collection_name):
        self.twitter_db.update_twitter_uploads_s3( id_str = id_str,
                                                   collection_name = collection_name)
   
    def create_file(self, collection_name):

      for user_name in self.students.keys():
        tweets = self.twitter_db.find_raw_data_users_by_username( user_name=user_name,
                                                                  collection_name=collection_name)

        for tweet in tweets:
            try:
               tweet_formatted = self.to_string(tweet)+'status_students='+self.students[user_name]
               tweet_formatted = econding_utils.clear_coding(txt=tweet_formatted)
               self.write_file(path_file=self.path_file,
                               content=tweet_formatted.encode('utf-8') )
               self.update_document_tweet( tweet['id_str'], collection_name ) ##flag
               self.count += 1
            except Exception as e:
               print 'fail: %s' % e
               break
     
    def run(self):
      
        print 'salvando raw_data em: %s' %  self.path_file
        for name in self.collections_names:
            print 'collection name %s' % name
            self.create_file( name )
            
        file_name =  'raw_data/raw_data_students/'+self.current_time+'/'+ self.file_name
        s3 = S3Connector()
        print 'realizando upload..'
        self.save_logs_s3(text='Enviando arquivos para Aws S3')

        s3.upload_file( bucket_name=self.bucket_name,
                        file_name = file_name,
                        path_file=self.path_file )
        print 'upload completo..'
        s3_path_name = 's3n://'+self.bucket_name+'/'+file_name
        self.save_s3_jobs_upload(file_name=file_name,
                                 s3_path_name=s3_path_name,
                                 name='raw_data_students')

        self.save_logs_s3(text='Documentos importados para Aws S3 : total %s em %s minutos' % \
                                       (self.count, date_utils.diff_data_minute( self.start )))