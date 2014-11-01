#!/usr/bin/python
# -*- coding: utf-8 -*-
from unicodedata import normalize

class AnalyzerPossibleStudentAbstract( object ):

    def clear_coding(self, txt, codif='utf-8'):
        try:
            return normalize('NFKD', txt.decode(codif)).encode('ASCII','ignore')
        except Exception as ex:
            return txt

    def filter(self, json_analyzer):
        raise NotImplementedError()

    def get_name(self):
       raise NotImplementedError()


class AnalyzerPossibleStudentTwitter( AnalyzerPossibleStudentAbstract ):

    def __init__(self):
        AnalyzerPossibleStudentAbstract.__init__(self)
        self.__point_for_twitter__()

    def get_name(self):
        return 'twitter'

    def __point_for_twitter__(self):
        possible_student_dictionary = { 'vestibular' : 5, 'unipam': 3,
                                        'prova' : 5, 'vest' : 5,
                                        'passar': 5, 'podia passar': 5,
                                        'inscricao' : 5, 'curso'  : 5,
                                        'enem' : 5, 'fazer' : 5,
                                        'la na unipam' : 5, 'colegio' : 5,
                                        'normal' : 3, 'marcolino' : 5,
                                        'fpm' : 5, 'faculdade' : 3
                                       }

        student_dictionary = { 
                                'unipam': 5, 'na unipam' : 5,
                                'no unipam' : 5, 'no comine' : 5,
                                'comine': 5, 'inscricao' : 5,''
                                'curso'  : 5, 'bloco'  : 5,
                                'pontos'  :5,
                            }

        invalid_users = ['patoshoje', 'CDLPATOS', 'patos_agora', 'portaltopgyn',
                            'oqrola', 'COPASA115', 'g1tvintegracao', 'UNIPAMNET', ]  

        setattr(self,'possible_student_dictionary', possible_student_dictionary)
        setattr(self,'student_dictionary', student_dictionary)
        setattr(self,'invalid_users', invalid_users)



    def filter(self, json_analyzer):
        
        text = json_analyzer['text']
        user_name = json_analyzer['user']['screen_name']

        if user_name not in self.invalid_users:
            words_encondig  = [ self.clear_coding( word ) 
                        for word in text.split(' ') ]
           
            points_possible_student, points_student = 0 , 0
            for word in words_encondig:
                if self.possible_student_dictionary.has_key( word.lower() ):
                   points_possible_student +=  self.possible_student_dictionary[ word.lower() ]

                if self.student_dictionary.has_key( word.lower() ):
                   points_student +=  self.student_dictionary[ word.lower() ]
            
            if points_possible_student > 3 and points_possible_student > points_student:
                return 'POSSIVEL_ESTUDANTE'
            elif points_student > 3:
                return 'ESTUDANTE'
        return None
