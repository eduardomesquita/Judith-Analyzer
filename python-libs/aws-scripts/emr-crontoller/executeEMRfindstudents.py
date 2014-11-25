from emrcontroller import *
import sys
path_python_libs =  '/'.join( sys.path[0].split('/')[:-2] )
sys.path.append(path_python_libs + '/python-libs/utils/')
import dateutils as date_utils


class ExecuteEMRFindStudents( EMRController ):

    def __init__(self, name ):
        EMRController.__init__(self, name_config_emr=name, script_mapper_name=name )
        setattr(self,'path_uploads_s3','all_data')


    def download_s3(self):
       print 'inicando download arquivos S3..'
       files = self.download_result()

       print 'download completo..\nsalvando dados..'
       imports_count= self.read_files_s3( files=files )
       return imports_count


    def save_files_s3(self, line):
      line = line.strip()
      (text, count) = line.split('\t')
      (status,user_name) = text.split(';')
      self.analyzer_db.save_students( user_name=user_name,status=status,count=count,
                                      create_at=date_utils.current_time())
         

    def start_map_reduce(self):
        script_name = __name__

        for input_file in self.get_path_input_file( name=self.path_uploads_s3, script_name=script_name): 
          state = self.execute_emr( input_file, script_name )

        return state