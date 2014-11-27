from abstract.analyzerabstract import *
from sets import Set
import time, platform
from datetime import datetime
from collections import OrderedDict

if  platform.node() == 'eduardo-linux':
        PATH = '/home/eduardo/Projetos/TCC/python-libs/analyzers/dictionary/dictionary.txt'
else:
        PATH ='/home/eduardo/Projetos/TCC/python-libs/analyzers/dictionary/dictionary.txt'


def read_dicinonary():
    words, courses  = {}, {}
    for word in open(PATH, 'r'):
        word = word.upper().strip()
        if '=' in word:
            ( _, word ) = word.split('=')
            courses[word] = 1
        words[ word ] = 1
    return words.keys(), courses.keys()


class TweetAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        setattr(self, 'all_word', {})
        setattr(self, 'status_user_word', {})
        setattr(self, 'location_word', {})
        setattr(self, 'course_word', {})
        setattr(self, 'course_word_status', {})
        setattr(self, 'course_word_location', {})
        
        dicionary, dicionary_courses = read_dicinonary()

        setattr(self, 'dicionary', dicionary)
        setattr(self, 'dicionary_courses', dicionary_courses)


    def __aggretation_word__(self, word, count, **kargs):
       return self.sum_dict(word, count, **kargs)
       
    def __aggretation_word_status__(self, word, count, status, **kargs):
        if not kargs.has_key( status ):
            kargs[ status ] = {}
        kargs[ status ] = self.sum_dict(word, count,**kargs[ status ])
        return kargs

    def __aggretation_word_status_location__(self, word, count, status, location, **kargs):
        if not kargs.has_key( status ):
            kargs[status] = {}

        if not kargs[status].has_key( location ):
            kargs[status][location] = {}
        kargs[ status ][location] = self.sum_dict(word, count,**kargs[ status ][location])
        return kargs



    def sum_dict(self, key, count, **kargs):    
        if kargs.has_key( key ):
            kargs[ key ] += int(count)
        else:
            kargs[ key ] = int(count)
        return kargs

    def sort_by_values(self, **dicts):
        return OrderedDict(sorted(dicts.items(), key=lambda t: t[1]))

    def emit_students(self, word, count, status, location):
        if word in self.dicionary: ##dicionario words
            self.all_word = self.__aggretation_word__( word, count, **self.all_word)

            if status != 'None' and status != '' and status != None:
                self.status_user_word = self.__aggretation_word_status__(word,count, 
                                                                         status, **self.status_user_word)

                if location != 'None' and location != '' and location != None:
                    self.location_word= self.__aggretation_word_status_location__(word,count,
                                                                                  status, location,
                                                                                  **self.location_word)

    def emit_courses(self, word, count, status, location):
        if word in self.dicionary_courses: ## dicionario cursos
            self.course_word = self.__aggretation_word__( word, count, **self.course_word)

            if status != 'None' and status != '' and status != None:
                self.course_word_status = self.__aggretation_word_status__(word,count, 
                                                                           status, **self.course_word_status)
            
                if location != 'None' and location != '' and location != None:
                    self.course_word_location=self.__aggretation_word_status_location__(word,count,
                                                                                        status, location,
                                                                                        **self.course_word_location)

    def emit(self, bjson):
        word = bjson['word'].upper()
        count = bjson['count']
        status = bjson['statusStudents']
        location = bjson['location'].upper()
        self.emit_students(word=word, count=count, status=status, location=location)
        self.emit_courses(word=word, count=count, status=status, location=location)


    def get_raw_data(self, projection = {'_id':0, 'created_at':0}):
        for bjson in self.analyzer_db.get_raw_data_tweets( projection ):
             self.emit(bjson)

    def sort_limit_10_great(self, dicts_tmp):
        lista_return = {}
        for key in  dicts_tmp.keys()[::-1]:
           if len(lista_return.keys()) < 10:
                lista_return[key] = dicts_tmp[key] 
        return lista_return

    def sort_limit_3_great(self, dicts_tmp):
        lista_return = {}
        for key in  dicts_tmp.keys()[::-1]:
            if len(lista_return.keys()) < 3:
                lista_return[key] = dicts_tmp[key] 
        return lista_return

    def get_values_word(self, **kargs):
        kargs = self.sort_by_values( **kargs )
        return  self.sort_limit_10_great( kargs )

    def get_values_status(self, **kargs):
        for key in kargs.keys():
            kargs[key] = self.sort_by_values( **kargs[key])
            kargs[key] = self.sort_limit_10_great( kargs[key] )
        return kargs

    def get_values_location(self, **kargs):
        for key in kargs.keys():
            for location in kargs[key].keys():
               kargs[key][location] =  self.sort_by_values( **kargs[key][location])
               kargs[key][location] = self.sort_limit_3_great( kargs[key][location] )
        return kargs

    def init(self):
        
        print 'Start cache TweetAnalyzer..'

        self.get_raw_data()

        self.all_word = self.get_values_word( **self.all_word )
        self.course_word = self.get_values_word( **self.course_word )
     
        self.status_user_word = self.get_values_status( **self.status_user_word )
        self.course_word_status = self.get_values_status( **self.course_word_status )

      


        self.location_word = self.get_values_location( **self.location_word )
        self.course_word_location =  self.get_values_location( **self.course_word_location)
       
        for i in self.location_word:
            print i

        #self.analyzer_db.save_cache_data('word_count_status_user', **self.status_user_word ) 
        #self.analyzer_db.save_cache_data('word_count_all_students', **self.all_word ) 
        #self.analyzer_db.save_cache_data('word_count_status_location_user', **self.location_word )
        #self.analyzer_db.save_cache_data('word_count_course_word', **self.course_word )
        #self.analyzer_db.save_cache_data('word_course_word_status', **self.course_word_status )
        #self.analyzer_db.save_cache_data('word_course_word_location', **self.course_word_location )

        print 'Fim cache TweetAnalyzer..'
        

if __name__ == '__main__':
    TweetAnalyzer().init()