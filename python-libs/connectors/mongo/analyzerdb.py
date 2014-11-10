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

    def save_students(self, user_name, status, count):
        json_save = { 'user_name': user_name, 
                      'status_students': status,
                      'count':count,
                      'create_at': date_utils.current_time()}

        self.save( json_save=json_save, 
                   collection_name=self.default_students_collections_name())