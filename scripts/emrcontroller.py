import sys, os
path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/python-libs/connectors/mongo/')
sys.path.append(path_python_libs + '/python-libs/aws-api/')
from configdb import ConfigDB
from analyzerdb import AnalyzerDB
from mapreducelib import AwsMapReduce
from s3lib import S3Connector

class EMRController(object):

    def __init__(self):
        setattr(self, 'config_db', ConfigDB())
        setattr(self, 'aws_map_reduce', AwsMapReduce())
        setattr(self, 's3_connector', S3Connector())
        setattr(self, 'analyzer_db', AnalyzerDB())

    def start_map_reduce(self):
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

    def default_collection_mapper(self):
        return 'scriptsMapper'

    def default_s3_collection_name(self):
        return 'uploads_s3'

    def get_path_script_mapper(self,name, db):
        return db.find( json_find = {'name': name}, 
                         collection_name = self.default_collection_mapper())

    def get_path_input_file(self, name, db):
        cursor = db.find( json_find = {'name': name }, 
                         collection_name = self.default_s3_collection_name())
        paths = []
        for data in cursor:
            paths.append(data['path_s3_name'])
        if paths:
            return paths
        raise Exception('sem atualizacao em seu s3')
     
    def get_output_path(self, input_file):
        return input_file.replace('/raw_data/', '/output/').replace('raw_data_twitter', '')

    def get_logs_path(self, input_file):
        return input_file.replace('/raw_data/', '/logs/').replace('raw_data_twitter', '')



    def download_result_map_reduce(self, output_files, folder_name):

        AWS_PATH = list(self.config_db.get_config('path_aws_download'))[0]['value']
        
        files = []
        bucket_name = self.__get_bucket_name__()
        for k in self.s3_connector.list_content_of_bucket(bucket_name=bucket_name):
            keys_string =  str( k.key ) 
            for output in output_files:
                file_output = output.replace('s3n://%s/' % (bucket_name), '')
                if file_output in keys_string:
                    self.__create_dirs_download__( AWS_PATH=AWS_PATH,
                                                  folder_name=folder_name,
                                                  aws_path=keys_string )

                    LOCAL_PATH = AWS_PATH+keys_string
                    print 'realizadno download: %s' % LOCAL_PATH
                    if not os.path.exists(LOCAL_PATH):
                        k.get_contents_to_filename(LOCAL_PATH)
                        files.append(LOCAL_PATH)
                        print 'download completo..'
                    else:
                        print 'arquivo existente..'
        return files

