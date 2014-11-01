from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
from mapreducelib import AwsMapReduce
from datetime import *
import credentialsAWS as credentials_AWS
import  sys, os

path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/connectors/mongo/')
from mongojudith import TwitterDB

class S3Connector(object):
    def __init__(self, **kwargs):  
        setattr(self, 'conn', S3Connection( kwargs['AWSAccessKeyId'], kwargs['AWSSecretKey']))

    def __show_region_name__(self):
        print '\n'.join(i for i in dir(Location) if i[0].isupper())

    def create_bucket(self, bucket_name):
        try:
            return self.conn.create_bucket( bucket_name, location=Location.SAEast )
        except Exception as ex:
            raise Exception( ex )

    def get_bucket(self, bucket_name):
        return self.conn.get_bucket( bucket_name )

    def set_string(self, bucket_name, folder, key_name, text ):
        bucket = self.get_bucket( bucket_name )
        k = Key( bucket )
        k.key = folder +'/'+ key_name
        k.set_contents_from_string( text )

    def upload_file(self, bucket_name, file_name, path_file ):
        bucket = self.get_bucket( bucket_name )
        k = Key( bucket )
        k.key = file_name
        k.set_contents_from_filename(path_file)

    def get_s3_file_name(self):
        fmt = "%Y-%m-%d %H:%M:%S"
        data_atual =  datetime.now().strftime( fmt )
        return data_atual.replace(' ', '-').replace(':', '-')

    def create_mapreduce_for_students_and_possible(self):
        raise NotImplementedError()


class TwitterS3Connector( S3Connector ):

    def __init__(self):
        S3Connector.__init__(self, **USER_AUTH)
        setattr(self, 'twitter_db', TwitterDB())

   
    def __create_dir__(self, path_aws_uploads):
        path_folder = path_aws_uploads['value']
        folder_name = self.get_s3_file_name()
        os.mkdir( path_folder +  folder_name + '/') 
        return  path_folder, folder_name

    def __create_file_upload__(self, file_name = 'raw_twitters_students.txt'):
        
        path_aws_uploads = list( self.twitter_db.get_config('path_aws_uploads') )[0]

        path_folder, folder_name = self.__create_dir__( path_aws_uploads )
        path_file = path_folder  + folder_name +'/'+ file_name
        print 'iniciando s3 twitter...'
        print 'tentando gravar dados em %s' % ( path_file )

        file_upload = open(path_file, 'a')
        data_save = 0
        for data in self.twitter_db.find_raw_data_users():
            try:
                tweet =  data['text'].encode('utf-8').replace(';','')
                user_name =  data['user']['screen_name'].encode('utf-8')
                file_upload.write( tweet+';'+user_name+ '\n' )
                data_save += 1
            except Exception as ex:
                print 'erro ao salvar arquivos %s - %s' % (ex, data)
        print 'Total data_raws salvos: %s' % (data_save)
        file_upload.close()

        if data_save > 0:
            file_name = 'students_and_possible/' + folder_name +'/'+file_name
            return file_name , path_file, folder_name
        else:
            raise 'dados nao foram salvos..'


    def create_mapreduce_for_students_and_possible(self):
        file_name, path_file, folder_name =  self.__create_file_upload__()
        print 'fazendo upload s3..'
        params = list( self.twitter_db.get_config('aws_bucket_name') )[0]
        bucket_name = params['value']
      
        self.upload_file( bucket_name=bucket_name, 
                          file_name = file_name, 
                          path_file= path_file )

        print 'upload concluido com sucesso..'
        print 'iniciando map reduce...\n\n'
        map_recude = AwsMapReduce()
        output = 'students_and_possible/' + folder_name + '/output/twitter/'
        logs = 'students_and_possible/' + folder_name + '/logs/twitter/'

        map_recude.create(  map_reduce_name='students_and_possible '+folder_name,
                            file_input='s3n://'+bucket_name+'/'+file_name,
                            file_output='s3n://'+bucket_name+'/'+output,
                            log_file='s3n://'+bucket_name+'/'+logs,
                            n_instance = 1)
        
USER_AUTH = credentials_AWS.read_credential()

tw  = TwitterS3Connector()
tw.create_mapreduce_for_students_and_possible()

