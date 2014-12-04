from abstract.analyzerabstract import *
from sets import Set
import time, platform
from datetime import datetime
from collections import OrderedDict

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


def replace_name( name):

        if name == 'ADM':
           return 'ADMINISTRACAO'
        if name == 'AMBIENTAL':
            return 'ENG AMBIENTAL'
        if name == 'cienciasbiologicas'.upper():
            return 'Cienc Biologicas'
        if name == 'biologicas'.upper():
            return 'Cienc Biologicas'
        if name == 'biologia'.upper():
            return 'Cienc Biologicas'
        if name == 'biologia'.upper():
            return 'Cienc Biologicas'
        if name == 'cienciascontabeis'.upper():
            return 'Cienc Contabeis'
        if name == 'contabeis'.upper():
            return 'Cienc Contabeis'
        if name == 'educacaofisica'.upper():
            return 'Edu Fisica'
        if name == 'educacao fisica'.upper():
            return 'Edu Fisica'
        if name == 'engenharia'.upper():
            return 'Eng Civil'
        if name == 'ambiental'.upper():
            return 'Eng Ambiental'
        if name == 'engenhariaambiental'.upper():
            return 'Eng Ambiental'
        if name == 'engenhariacivil'.upper():
            return 'Eng Civil'
        if name == 'eletrica'.upper():
            return 'Eng Eletrica'
        if name == 'engenhariaeletrica'.upper():
            return 'Eng Eletrica'
        if name == 'mecanica'.upper():
            return 'Eng Mecanica'
        if name == 'engenhariamecanica'.upper():
            return 'Eng Mecanica'
        if name == 'producao'.upper():
            return 'Eng producao'
        if name == 'engenhariadeproducao'.upper():
            return 'Eng producao'
        if name == 'engenhariadeproducao'.upper():
            return 'Eng producao'
        if name == 'engenharia quimica'.upper():
            return 'Eng quimica'
        if name == 'engenhariaquimica'.upper():
            return 'Eng quimica'
        if name == 'gestaocomercial'.upper():
            return 'gestao comercial'
        if name == 'publicidade'.upper():
           return 'PUBLICIDADE E PROPAGANDA'
        if name == 'publicidadeepropaganda'.upper():
           return 'PUBLICIDADE E PROPAGANDA'
        if name == 'publicidade'.upper():
           return 'PUBLICIDADE E PROPAGANDA'
        if name == 'propaganda'.upper():
           return 'PUBLICIDADE E PROPAGANDA'
        if name == 'sistemas'.upper():
           return 'SISTEMAS DE INFORMACAO'
        if name == 'sistemasdeinformacao'.upper():
           return 'SISTEMAS DE INFORMACAO'
        return name

class TweetAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        dicionary, dicionary_courses = read_dicinonary()
        setattr(self, 'dicionary', dicionary)
        setattr(self, 'dicionary_courses', dicionary_courses)

    def __aggretation_word__(self, word, count, **kargs):
       return self.sum_dict(word, count, **kargs)

       
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
            #self.all_word = self.__aggretation_word__( word, count, **self.all_word)

            if status != 'None' and status != '' and status != None:
                #self.status_user_word = self.__aggretation_word_status__(word,count, 
                #                                                        status, **self.status_user_word)
                
                if location != 'None' and location != '' and location != None:
                    #self.location_word= self.__aggretation_word_status_location__(word,count,
                     #                                                             status, location,
                     #                                                             **self.location_word)
                     pass 

    def emit_courses(self, word, count, status, location):
        if word in self.dicionary_courses: ## dicionario cursos
            word = replace_name( word ).upper()
            #self.course_word = self.__aggretation_word__( word, count, **self.course_word)

            if status != 'None' and status != '' and status != None:
                self.__aggretation_word_status__( word,count, status, name='word_course_word_status')
            
                #if location != 'None' and location != '' and location != None:
                #    self.course_word_location=self.__aggretation_word_status_location__(word,count,
                #                                                                        status, location,
                #                                                                        **self.course_word_location)
                pass

    def emit(self, bjson):
        word =  bjson['word'].upper() 
        count = bjson['count']
        status = bjson['statusStudents']
        location = bjson['location'].replace('.','')

        #self.emit_students(word=word, count=count, status=status, location=location)
        self.emit_courses(word=word, count=count, status=status, location=location)


    def get_raw_data(self, projection = {'_id':0, 'created_at':0}):
        for bjson in self.analyzer_db.get_raw_data_tweets( projection ):
            self.emit(bjson)
        print 'terminei ...'


    def init(self):
        print 'Start cache TweetAnalyzer..'

        self.analyzer_db.remove_cache(name='word_course_word_status')
        
        self.get_raw_data()
        
        print 'Fim cache TweetAnalyzer..'


if __name__ == '__main__':
    TweetAnalyzer().init()