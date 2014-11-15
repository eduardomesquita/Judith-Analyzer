from pymongo import MongoClient
import json, pymongo
from mongojudith import *
import sys

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

    def save_students(self, user_name, status, count, create_at):
        data = { 'userName': user_name, 
                 'statusStudents': status,
                 'count':count,
                 'create_at': create_at }

        self.save( data=data, 
                   collection_name=self.default_students_collections_name())
