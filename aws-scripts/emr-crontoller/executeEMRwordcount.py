from emrcontroller import *
import sys
path_python_libs =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(path_python_libs + '/python-libs/utils/')
import dateutils as date_utils


class ExecuteEMRWordCount( EMRController ):

    def __init__(self):
        EMRController.__init__(self)

    def __get_script__(self):
        return list( self.get_path_script_mapper( name='find_students',
                                                  db=self.config_db))[0]['value']

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


    def start_map_reduce(self, emr_name, n_instance):
        script_name = self.__get_script__()

        input_files = self.get_path_input_file( name='raw_data_students',
                                                db=self.config_db,
                                                script_name=emr_name)
        
        imports_count = 0
        start = date_utils.current_time()

        for input_file in input_files:

            output_file = self.get_output_path( input_file )
            log_file = self.get_logs_path( input_file )



            print 'output: ' + output_file
            print 'log: ' + log_file
            print 'criando map reduce.. com %s instancias' % n_instance
           
         #   (state, job_id) = self.aws_map_reduce.create( name = 'teste',
         #                                                 input_file=input_file,
         #                                                 output_file=output_file,
         #                                                 log_file=log_file,
         #                                                 mapper=script_name,
         #                                                 n_instance = n_instance)
            
         #  print '\n\n'
         #  if state == 'COMPLETED':
         #       imports_count=self.download_s3( output_file=output_file,
         #                                      script_name=emr_name,
         #                                       path_s3_name=input_file)
            
         #   qtd_minutos = date_utils.diff_data_minute( start )
         #   log_map_reduce = { 'state':state,
         #                      'jobId':job_id,
         #                      'date':date_utils.current_time(),
         #                      'scriptName':emr_name,
         #                      'inputFile':input_file,
         #                      'runTimeMinutes':qtd_minutos,
         #                      'logFile' : log_file,
         #                      'outputFile':output_file, 
         #                      'CountDataImport':imports_count }

         #   print 'salvando jobs Map Reduce'
         #   self.save_jobs_emr( **log_map_reduce )



if __name__ == '__main__':
    if  len(sys.argv) == 1:
        ExecuteEMRWordCount().start_map_reduce(sys.argv[0], 1)
    else:
        ExecuteEMRWordCount().start_map_reduce(sys.argv[0], sys.argv[1])
