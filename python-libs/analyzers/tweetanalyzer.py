from abstract.analyzerabstract import *
from sets import Set
import time, platform, sys
from datetime import datetime
from collections import OrderedDict

current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(current_dir + '/utils/')
import replaceutils as replace_utils

if  platform.node() == 'eduardo-linux':
        PATH = '/home/eduardo/Projetos/judith-project/judith-controllers/python-libs/analyzers/dictionary/dictionary.txt'
else:
        PATH ='/home/ubuntu/judith-project/python-libs/analyzers/dictionary/dictionary.txt'


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
        dicionary, dicionary_courses = read_dicinonary()
        setattr(self, 'dicionary', dicionary)
        setattr(self, 'dicionary_courses', dicionary_courses)

    def __aggretation_word__(self, word, count, name):
        data = list(self.analyzer_db.find_cache_data( **{'name' : name} ))
        if len(data) == 0: 
            self.analyzer_db.save_cache_data( **{ 'name' : name,
                                                  'word':word, 'count':count })  
        else:
            for aggretation in  data:
                if aggretation['word'] == word:
                    updated = int(aggretation['count']) + int(count)
                    match_criteria = { 'name' : name, 'word':word}
                    self.analyzer_db.update_cache_data( match_criteria , { 'count':updated })
                    return 0

            self.analyzer_db.save_cache_data( **{ 'name' : name,
                                                  'word':word, 'count':count })

       
    def __aggretation_word_status__(self, word, count, status, name):
        data = list(self.analyzer_db.find_cache_data( **{'name' : name, 'status': status} ))
        if len(data) == 0: 
            self.analyzer_db.save_cache_data( **{ 'name' : name, 
                                                  'status' : status,
                                                  'word':word, 'count':count })  
        else:
            for aggretation in  data:
                if aggretation['word'] == word:
                    updated = int(aggretation['count']) + int(count)
                    match_criteria = { 'name' : name, 'status' : status,'word':word}
                    self.analyzer_db.update_cache_data( match_criteria , { 'count':updated })
                    return 0

            self.analyzer_db.save_cache_data( **{ 'name' : name, 
                                                  'status' : status,
                                                  'word':word, 'count':count })


    def __aggretation_word_status_location__(self, word, count, status, location, name):
        print word, count, status, location

        data = list(self.analyzer_db.find_cache_data( **{'name' : name, 'status': status, 'location' : location} ))
        if len(data) == 0:
            self.analyzer_db.save_cache_data( **{ 'name' : name, 
                                                  'status' : status,
                                                  'word':word, 
                                                  'location' : location,
                                                  'count':count })
        else:
            for aggretation in  data:
                if aggretation['word'] == word:
                    updated = int(aggretation['count']) + int(count)
                    match_criteria = { 'name' : name, 'status' : status,'word':word}
                    self.analyzer_db.update_cache_data( match_criteria , { 'count':updated })
                    return 0

            self.analyzer_db.save_cache_data( **{ 'name' : name, 
                                                  'status' : status,
                                                  'word':word, 
                                                  'location' : location,
                                                  'count':count })


    def emit_students(self, word, count, status, location):
        if word in self.dicionary: ##dicionario words
            word = replace_utils.replace_word( word ).upper()
            self.__aggretation_word__( word, count, name='word_count')

            if status != 'None' and status != '' and status != None:
                self.__aggretation_word_status__( word,count, status, name='word_status')
                
                if location != 'None' and location != '' and location != None:
                    self.__aggretation_word_status_location__(word, count, status,
                                                             location, name='word_location_status')


    def emit_courses(self, word, count, status, location):
        if word in self.dicionary_courses: ## dicionario cursos
            word = replace_utils.replace_word( word ).upper()
            self.__aggretation_word__( word, count, name='word_course')

            if status != 'None' and status != '' and status != None:
                self.__aggretation_word_status__( word,count, status, name='word_course_status')
            
                if location != 'None' and location != '' and location != None:
                    self.__aggretation_word_status_location__(word, count, status, location, name='word_course_location_status')


    def emit(self, bjson):
        word =  replace_utils.remove_non_ascii_chars(bjson['word'].upper()) 
        status = replace_utils.remove_non_ascii_chars(bjson['statusStudents'])
        location = replace_utils.remove_non_ascii_chars(bjson['location'].replace('.',''))
        location = replace_utils.clean_location(location.upper())
        count = bjson['count']

        self.emit_students(word=word, count=count, status=status, location=location)
        self.emit_courses(word=word, count=count, status=status, location=location)


    def get_raw_data(self, projection = {'_id':0, 'created_at':0}):
        for bjson in self.analyzer_db.get_raw_data_tweets( projection ):
            self.emit(bjson)
        print 'terminei ...'


    def init(self):
        print 'Start cache TweetAnalyzer..'

        self.analyzer_db.remove_cache(name='word_course')
        self.analyzer_db.remove_cache(name='word_course_status')
        self.analyzer_db.remove_cache(name='word_course_location_status')
        self.analyzer_db.remove_cache(name='word_count')
        self.analyzer_db.remove_cache(name='word_status')
        self.analyzer_db.remove_cache(name='word_location_status')        
        self.get_raw_data()
        print 'Fim cache TweetAnalyzer..'

        
if __name__ == '__main__':
    TweetAnalyzer().init()