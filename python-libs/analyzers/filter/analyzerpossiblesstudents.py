#!/usr/bin/python
# -*- coding: utf-8 -*-
from unicodedata import normalize
import re

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
                                        'prova' : 3, 'vest' : 5,
                                        'passar': 3, 'inscricao' : 3,
                                        'curso'  : 2,'enem' : 7, 'fazer' : 2,
                                        'colegio' : 2,'normal' : 1,
                                        'marcolino' : 5,'fpm' : 2,
                                        'faculdade' : 4, 'zama' : 1,
                                        'maciel' : 1, 'patos' : 3,
                                        'bloco' : 2, 'comprovante' : 3,
                                        'fazer' : 3,'turma':3, 'abrir' : 1,
                                       }
        student_dictionary = { 
                                'unipam': 5, 'fepam' : 5,
                                'comine': 5, 'bloco'  : 5,
                                'portal' : 2, 'aula' : 3,
                                'palestra':2, 'administracao' : 3,
                                'agronegocio' : 3,'agronomia' : 3,
                                'arquitetura' : 3,'urbanismo' : 3,
                                'ciencias biologicas' : 3,'cienciasbiologicas' : 3,
                                'biologia' : 3,'cienciascontabeis' : 3,
                                'contabeis' : 3,'direito' : 3, 'adm' : 3,
                                'educacao fisica' : 3,'educacaofisica' : 3,
                                'enfermagem' : 3,'engenharia' : 3,
                                'engenharia ambiental' : 3,'engenhariaambiental' : 3,
                                'engenharia civil' : 3,'engenhariacivil' : 3,
                                'engenharia eletrica' : 3,'engenhariaeletrica' : 3,
                                'engenharia mecanica' : 3,'engenhariamecanica' : 3,
                                'engenharia de producao' : 3,'engenhariadeproducao' : 3,
                                'engenhariaproducao' : 3,'engenharia quimica' : 3,
                                'engenhariaquimica' : 3,'farmacia' : 3,
                                'fisioterapia' : 3,'gestao comercial' : 3,
                                'gestaocomercial' : 3,'historia' : 3,
                                'jornalismo' : 3,'Letras' : 3,
                                'medicina' : 3,'nutricao' : 3,
                                'pedagogia' : 3,'psicologia' : 3,
                                'publicidade' : 3,'publicidadeepropaganda' : 3,
                                'publicidade e propaganda' : 3,
                                'sistemas de informacao' : 3,'sistemasdeinformacao' : 3,
                                'sistemas' : 3,'Zootecnia' : 3, 'xicomine' : 5, 'unipamnet' : 5,
                            }


        invalid_users = ['patoshoje', 'CDLPATOS', 'patos_agora', 'portaltopgyn',
                            'oqrola', 'COPASA115', 'g1tvintegracao', 'UNIPAMNET','DURANDU85065556'
                            'marl3nemarta', 'HeybetaXis', 'bittersteel01', 'lauradiniz9655', 'DanielleC20013', 
                            'Guthiele','Novasdepaz_87fm', 'radiohoje']

        setattr(self,'possible_student_dictionary', possible_student_dictionary)
        setattr(self,'student_dictionary', student_dictionary)
        setattr(self,'invalid_users', invalid_users)


    def points(self, word, pp_student, p_student):
        
        word = word.lower().split('#')
        pattern = re.compile("[a-z|A-Z]")

        for case in word:
            case = "".join(re.findall(pattern, case))
            if self.possible_student_dictionary.has_key( case ):
               pp_student +=  self.possible_student_dictionary[ case ]

            if self.student_dictionary.has_key( case  ):
               p_student +=  self.student_dictionary[ case ]

        return pp_student, p_student

    def filter(self, json_analyzer):
        
        text = json_analyzer['text']
        user_name = json_analyzer['user']['screen_name']

        if user_name not in self.invalid_users:
            words_encondig  = [ self.clear_coding( word ) 
                        for word in text.split(' ') ]
           
            points_possible_student, points_student = 0 , 0
            for word in words_encondig:
                p = self.points( word=word,
                                 pp_student=points_possible_student,
                                 p_student=points_student)
              
                (points_possible_student, points_student) = p              

            if points_possible_student > 5 and points_possible_student > points_student:
                return 'POSSIVEL_ESTUDANTE'
            elif points_student > 3:
                return 'ESTUDANTE'
        return None


if __name__ == '__main__':
   import sys
   python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )
   sys.path.append( python_libs_path + '/TCC/python-libs/connectors/mongo/' )
   from mongojudith import TwitterDB
   mongo = TwitterDB()
   for i in list(mongo.find({}, 'twittersTags')):
       a = AnalyzerPossibleStudentTwitter()
       r =  a.filter( i )
       if r == 'POSSIVEL_ESTUDANTE':
           print r