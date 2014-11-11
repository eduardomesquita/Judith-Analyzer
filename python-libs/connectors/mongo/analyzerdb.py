from pymongo import MongoClient
import json, pymongo
from mongojudith import *
import sys

path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/python-libs/utils/')
import dateutils as date_utils


class AnalyzerDB( MongoJudithAbstract ):

    def __init__(self, ):
        MongoJudithAbstract.__init__( self, db='judith-project-analyzer')

    def default_collection_name(self):
        raise NotImplementedError()

    def default_students_collections_name(self):
        return 'students'

    def get_raw_data_students(self):
       return self.find(match_criteria={},
                        collection_name=self.default_students_collections_name() )

    def save_students(self, user_name, status, count):
        data = { 'userName': user_name, 
                 'statusStudents': status,
                 'count':count,
                 'create_at': date_utils.current_time()}

        self.save( data=data, 
                   collection_name=self.default_students_collections_name())
