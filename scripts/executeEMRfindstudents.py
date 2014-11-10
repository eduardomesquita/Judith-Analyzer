from emrcontroller import *
import sys

class ExecuteEMRFindStudents( EMRController ):

    def __init__(self):
        EMRController.__init__(self)

    def __get_script__(self):
        return list( self.get_path_script_mapper( name='find_students',
                                                  db=self.config_db))[0]['value']

    def __download_files__(self, list_outputs_files):
        output_files_name = self.download_result_map_reduce(output_files=list_outputs_files,
                                                            folder_name='find-students')
        return output_files_name

    def __open_files__(self, list_files, job_id):
        for file_name in list_files:
            for line in open(file_name,'r'):
                line = line.strip()
                (text, count) = line.split('\t')
                (status,user_name) = text.split(';')
                self.analyzer_db.save_students( status, user_name, count )

    def start_map_reduce(self, emr_name, n_instance):
        script_name = self.__get_script__()
        input_files = self.get_path_input_file( name='all_data',
                                                db=self.config_db)

        list_outputs_files = []
        for input_file in input_files:
            output_file = self.get_output_path( input_file)
            log_file = self.get_logs_path( input_file )

            print 'output: ' + output_file
            print 'log: ' + log_file
            print 'criando map reduce.. com %s instancias' % n_instance
            state = 'COMPLETED'
            job_id = 1
            list_outputs_files.append( output_file )
            #(state, job_id) = self.aws_map_reduce.create( name = 'teste',
            #                                              input_file=input_file,
            #                                              output_file=output_file,
            #                                              log_file=log_file,
            #                                              mapper=script_name,
            #                                              n_instance = n_instance)
            print '\n\n'

            if state == 'COMPLETED':
               list_files = self.__download_files__(list_outputs_files)
               self.__open_files__( list_files, job_id )

if __name__ == '__main__':
    if  len(sys.argv) == 1:
        ExecuteEMRFindStudents().start_map_reduce(sys.argv[0], 1)
    else:
        ExecuteEMRFindStudents().start_map_reduce(sys.argv[0], sys.argv[1])
