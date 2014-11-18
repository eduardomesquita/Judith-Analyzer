import sys, os
current_dir =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(current_dir + '/python-libs/connectors/mongo/')
sys.path.append(current_dir + '/python-libs/aws-api/')
sys.path.append(current_dir + '/python-libs/utils/')

from configdb import ConfigDB
from analyzerdb import AnalyzerDB
from mapreducelib import AwsMapReduce
from s3lib import S3Connector
import dateutils as date_utils


class EMRController(object):

    def __init__(self, name_config_emr):
        setattr(self, 'config_db', ConfigDB())
        setattr(self, 's3_connector', S3Connector())
        setattr(self, 'analyzer_db', AnalyzerDB())
        
        cursor=self.config_db.get_config_emr(name=name_config_emr)
        setattr(self, 'aws_map_reduce', AwsMapReduce( cursor ))

    def start_map_reduce(self):
        raise NotImplementedError()

    def download_s3(self):
        raise NotImplementedError()

    def __get_script__(delf):
        raise NotImplementedError()

    def __get_bucket_name__(self):
        return list( self.config_db.get_config('aws_bucket_name') )[0]['value']

    def __create_dirs_download__(self, AWS_PATH, folder_name, aws_path):
        for folder in aws_path.split('/')[:-1]:
            try:
                os.mkdir(AWS_PATH+folder)
            except OSError as e:
                pass
            AWS_PATH = AWS_PATH+folder+'/'
        return AWS_PATH

    def save_jobs_emr(self, **kargs):
        self.config_db.save_jobs_upload_EMR( **kargs )
        self.config_db.save_dust_analyzer( **kargs )

    def default_collection_mapper(self):
        return 'scriptsMapper'

    def default_s3_collection_name(self):
        return 'uploads_s3'

    def get_path_script_mapper(self,name, db):
        return db.find( match_criteria = {'name': name}, 
                        collection_name = self.default_collection_mapper())

    def get_path_input_file(self, name, db, script_name):
        cursor = db.find( match_criteria = {'name': name, 'scriptsReader':{'$nin' : [script_name]}}, 
                          collection_name = self.default_s3_collection_name())
        paths = []
        for data in cursor:
            paths.append(data['pathS3Name'])
        
        print 'sem atualizacao em seu s3'
        return paths
   
    def get_output_path(self, input_file):
        return input_file.replace('/raw_data/', '/output/').replace('raw_data_twitter', '')

    def get_logs_path(self, input_file):
        return input_file.replace('/raw_data/', '/logs/').replace('raw_data_twitter', '')


    def set_script_reader(self, script_name, path_s3_name ):
        self.config_db.update_jobs_scripts_s3( script_name=script_name,
                                               path_s3_name=path_s3_name)


    def download_result_map_reduce(self, output_file, folder_name):

        AWS_PATH = list(self.config_db.get_config('path_aws_download'))[0]['value']
        files = []
        bucket_name = self.__get_bucket_name__()
        for k in self.s3_connector.list_content_of_bucket(bucket_name=bucket_name):
            keys_string =  str( k.key ) 
            file_output = output_file.replace('s3n://%s/' % (bucket_name), '')

            if file_output in keys_string:
                self.__create_dirs_download__( AWS_PATH=AWS_PATH,
                                              folder_name=folder_name,
                                              aws_path=keys_string )

                LOCAL_PATH = AWS_PATH+keys_string
                print 'realizadno download: %s' % LOCAL_PATH
                if not os.path.exists(LOCAL_PATH):
                    k.get_contents_to_filename(LOCAL_PATH)
                    print 'download completo..'
                
                files.append( LOCAL_PATH )

        return files


