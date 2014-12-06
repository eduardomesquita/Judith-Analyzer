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


class EmrAbstract(object):

    def __init__(self, name_config_emr, script_mapper_name):
        setattr(self, 'config_db', ConfigDB())
        setattr(self, 's3_connector', S3Connector())
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'name_config_emr', name_config_emr)
        setattr(self, 'script_mapper_name', script_mapper_name)


    def start_map_reduce(self, input_file):
        raise NotImplementedError()

    def download_s3(self):
        raise NotImplementedError()

    def save_files_s3(self):
        raise NotImplementedError()

    def __init_atributes__(self, input_file):
        cursor=self.config_db.get_config_emr() 
        setattr(self, 'aws_map_reduce', AwsMapReduce( cursor ))

        setattr(self,'imports_count', 0)
        setattr(self,'start', date_utils.current_time())
        setattr(self,'output_file', self.__get_output_path__( input_file ))
        setattr(self,'log_file', self.__get_logs_path__( input_file ))
        emr_name,sequence = self.__job_name__( self.name_config_emr )
        setattr(self,'emr_name', emr_name)
        setattr(self,'sequence', sequence)


    def __get_parameters__(self, name):
        parms = list( self.config_db.get_parameters(key_name=name))
        return parms[0]['value']

    def __get_script_mapper__(self, name):

        script = list( self.config_db.get_path_aws_script_mapper(name=self.script_mapper_name))
        return script[0]['value']

    def __get_output_path__(self, input_file):
        return input_file.replace('/raw_data/', '/output/').replace('raw_data_twitter', '')

    def __get_logs_path__(self, input_file):
        return input_file.replace('/raw_data/', '/logs/').replace('raw_data_twitter', '')

    def __job_name__(self, name):
      sequence = list(self.config_db.get_sequence(name=name))
      return name + '-' + str(int(sequence[0]['sequence'])), int(sequence[0]['sequence'])

    def __set_script_reader__(self, script_name, path_s3_name ):
        self.config_db.update_jobs_scripts_s3( script_name=script_name,
                                               path_s3_name=path_s3_name)

    def __save_jobs_emr__(self, **kargs):
        self.config_db.save_jobs_upload_EMR( **kargs )
        self.config_db.save_dust_emr( **{ 'state-jobs':kargs['state'], 
                                          'emr_name':kargs['emr_name'], 
                                          'date':kargs['date'] } )


    def __done__(self, **kargs):
      self.__save_jobs_emr__( **kargs )
      self.__set_script_reader__( script_name=kargs['scriptName'],path_s3_name=kargs['pathS3Name'])
      self.config_db.update_sequence(name=self.name_config_emr, sequence=self.sequence)
      print 'done..'


    def __create_dirs_download__(self, PATH_DOWNLOAD, folder_name, aws_path):
        for folder in aws_path.split('/')[:-1]:
            try:
                os.mkdir(PATH_DOWNLOAD+folder)
            except OSError as e:
                pass
            PATH_DOWNLOAD = PATH_DOWNLOAD+folder+'/'
        return PATH_DOWNLOAD


    def __start__(self, input_file, script_name):

        (state, job_id) = self.aws_map_reduce.create(name=self.emr_name,
                                                     input_file=input_file,
                                                     output_file=self.output_file,
                                                     log_file=self.log_file,
                                                     mapper=self.__get_script_mapper__(name=script_name))
        setattr(self, 'state', state)
        setattr(self, 'job_id', job_id)

        self.save_logs_emr(text='Jobs Elastic MapReduce terminado com satus: %s' % (state))

        if self.state == 'COMPLETED':
            return self.download_s3()


    def get_path_input_file(self, name, script_name):
        cursor = self.config_db.get_path_aws_input_file( name=name, script_name=script_name)
        paths = []
        [ paths.append(data['pathS3Name']) for data in cursor ]
        if len(paths) == 0:
            print 'Sem atualizacao no S3'
        return paths

    def read_files_s3(self, files):
        imports_count = 0
        for file_name in files:
            for line in open(file_name,'r'):
                self.save_files_s3( line )
                imports_count += 1

        print 'dados importados.. %s' % imports_count
        return imports_count

    def execute_emr(self, input_file, script_name):

        self.__init_atributes__( input_file )

        self.save_logs_emr(text='Criando MapRreduce: Com %s instancias' %  self.aws_map_reduce.get_instance_number())

        imports_count = self.__start__( input_file, script_name )

        log_map_reduce = {  'state':self.state,
                            'jobId':self.job_id,
                            'date':date_utils.current_time(fmt='%d-%m-%Y %H:%M:%S'),
                            'inputFile':input_file,
                            'scriptName' : script_name, 
                            'runTimeMinutes': date_utils.diff_data_minute(self.start),
                            'logFile' : self.log_file,
                            'outputFile': self.output_file, 
                            'CountDataImport': imports_count,
                            'emr_name' : self.emr_name,
                            'nInstance' : self.aws_map_reduce.get_instance_number(),
                            'scriptMappper': self.__get_script_mapper__(name=script_name),
                            'typeMaster' : self.aws_map_reduce.get_instance_master(),
                            'typeSlaver' : self.aws_map_reduce.get_instance_slave(),
                            'region': self.aws_map_reduce.get_region(),
                            'cluster' : 'Hadoop',
                            'pathS3Name' : input_file}

        self.__done__( **log_map_reduce )
        return self.state
            
            
    def download_result(self):

        PATH_DOWNLOAD = self.__get_parameters__('path_aws_download')
        bucket_name   = self.__get_parameters__('aws_bucket_name')
        
        file_output = self.output_file.replace('s3n://%s/' % bucket_name, '')
        retorno = []
        
        for k in self.s3_connector.list_content_of_bucket(bucket_name=bucket_name):
            keys_string =  str( k.key )

            if file_output in keys_string:
                self.__create_dirs_download__( PATH_DOWNLOAD=PATH_DOWNLOAD, 
                                               folder_name=self.name_config_emr, 
                                               aws_path=keys_string )
                
                LOCAL_PATH = PATH_DOWNLOAD+keys_string

                print 'realizadno download s3 para: %s' % LOCAL_PATH

                if not os.path.exists(LOCAL_PATH):
                    k.get_contents_to_filename(LOCAL_PATH)
                    print 'download s3 completo..'
                retorno.append( LOCAL_PATH )

        return retorno

    def save_logs_emr(self, text):
        self.config_db.save_log( **{'text':text, 'date': date_utils.current_time(fmt='%d-%m-%Y %H:%M:%S'), 
                                    'type' : 'MAPREDUCE', 'JobsName' : self.emr_name,
                                    'scriptName'  : self.script_mapper_name} )

