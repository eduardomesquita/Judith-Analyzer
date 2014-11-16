from pymongo import MongoClient
import json, pymongo
from mongojudith import *
import sys, pymongo

class AnalyzerDB( MongoJudithAbstract ):

    def __init__(self, ):
        MongoJudithAbstract.__init__( self, db='judith-project-analyzer')

    def default_collection_name(self):
        raise NotImplementedError()

    def default_students_collections_name(self):
        return 'students'

    def default_students_collections_word_count(self):
        return 'wordCountStudents'

    def get_raw_data_students(self):
        return self.find(match_criteria={},
                        collection_name=self.default_students_collections_name() )

    def get_raw_data_tweets(self, projection):
        return self.find_projection(match_criteria={},
                                    projection=projection,
                                    collection_name=self.default_students_collections_word_count() )

    def save_students(self, user_name, status, count, create_at):
        try:
            data = { 'userName': user_name, 
                     'statusStudents': status,
                     'count':count,
                     'create_at': create_at }
            self.save( data=data, 
                       collection_name=self.default_students_collections_name())

        except pymongo.errors.DuplicateKeyError:
            match_criteria = {'userName': user_name,'statusStudents': status}
            values = {'count':count}
            self.update( match_criteria=match_criteria,
                         values=values,
                         collection_name=self.default_students_collections_name(),
                         upsert = False)


    def save_word_count(self, status_students, location, word, count, create_at):
        try:
            data = { 'word': word, 
                     'statusStudents': status_students,
                     'location': location,
                     'count':count,
                     'created_at': create_at }  
            self.save( data=data, 
                       collection_name=self.default_students_collections_word_count())

        except pymongo.errors.DuplicateKeyError:
            pass
