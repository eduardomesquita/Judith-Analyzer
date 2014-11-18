from emrcontroller import *
import sys
path_python_libs =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(path_python_libs + '/python-libs/utils/')
import dateutils as date_utils


class ExecuteEMRFindStudents( EMRController ):

    def __init__(self):
        EMRController.__init__(self, 'find-students')

    def __get_script__(self):
        return list( self.get_path_script_mapper( name='find_students',
                                                  db=self.config_db))[0]['value']

    def read_files_s3_and_save(self, list_files, function_save):
        imports_count = 0
        for file_name in list_files:
            for line in open(file_name,'r'):
                line = line.strip()
                (text, count) = line.split('\t')
                (status,user_name) = text.split(';')
                function_save( user_name=user_name,
                               status=status,
                               count=count,
                               create_at=date_utils.current_time())
                imports_count += 1
        print '%s estudantes salvos... ' % imports_count
        return imports_count

    def download_s3(self, output_file, script_name, path_s3_name):
       print 'baixando arquivos S3..'
       files = self.download_result_map_reduce(output_file=output_file,
                                               folder_name='find-students')
       print 'download completo..'
       print 'salvando dados..'

       imports_count= self.read_files_s3_and_save( list_files=files,
                                                  function_save=self.analyzer_db.save_students )
       
       self.set_script_reader( script_name=script_name,
                               path_s3_name=path_s3_name )
       return imports_count


    def start_map_reduce(self, script_name):
        imports_count = 0
        emr_name=self.aws_map_reduce.name+'-'+str(int(self.aws_map_reduce.count))
        start = date_utils.current_time()
        
        for input_file in self.get_path_input_file( name='all_data',
                                                    db=self.config_db,
                                                    script_name=script_name):

            output_file = self.get_output_path( input_file )
            log_file = self.get_logs_path( input_file )

            print 'output: ' + output_file
            print 'log: ' + log_file
            print 'criando map reduce.. com %s instancias' % int(self.aws_map_reduce.n_instance)
            print 'nome EMR: %s ' % emr_name
       
            (state, job_id) = self.aws_map_reduce.create( name=emr_name,
                                                          input_file=input_file,
                                                          output_file=output_file,
                                                          log_file=log_file,
                                                          mapper=self.__get_script__())
            print '\n\n'
            if state == 'COMPLETED':
                imports_count=self.download_s3( output_file=output_file,
                                                script_name=script_name,
                                                path_s3_name=input_file)
            
            qtd_minutos = date_utils.diff_data_minute( start )
            log_map_reduce = {  'state':state,
                                'jobId':job_id,
                                'date':date_utils.current_time(),
                                'scriptName':emr_name,
                                'inputFile':input_file,
                                'runTimeMinutes':qtd_minutos,
                                'logFile' : log_file,
                                'outputFile':output_file, 
                                'CountDataImport':imports_count,
                                'emr_name' : emr_name,
                                'instance': self.aws_map_reduce.n_instance,
                                'typeMaster' : self.aws_map_reduce.master_instance,
                                'typeSlaver' : self.aws_map_reduce.slaver_instance,
                                'region': self.aws_map_reduce.region,
                                'cluster' : 'Hadoop'}

            print 'salvando jobs Map Reduce'
            self.save_jobs_emr( **log_map_reduce )
            self.config_db.update_config_emr(name=self.aws_map_reduce.name,
                                             count=self.aws_map_reduce.count)



if __name__ == '__main__':
    if  len(sys.argv) == 1:
        ExecuteEMRFindStudents().start_map_reduce(sys.argv[0])
    else:
        ExecuteEMRFindStudents().start_map_reduce(sys.argv[0])
