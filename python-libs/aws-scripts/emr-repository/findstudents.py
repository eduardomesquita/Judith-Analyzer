#!/usr/bin/python
import re, sys


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

def points( word, pp_student, p_student):
    word = word.lower().split('#')
    pattern = re.compile("[a-z|A-Z]")

    for case in word:
        case = "".join(re.findall(pattern, case))
        if possible_student_dictionary.has_key( case ):
           pp_student +=  possible_student_dictionary[ case ]

        if student_dictionary.has_key( case  ):
           p_student +=  student_dictionary[ case ]

    return pp_student, p_student


def filter_tweet(text, user_name):

    points_possible_student = 0 
    points_student = 0

    for word in text.split(' '):
        pts = points( word=word,
                      pp_student=points_possible_student,
                      p_student=points_student)
      
        (points_possible_student, points_student) = pts              

        if points_possible_student > 5 and points_possible_student > points_student:
            print "LongValueSum:POSSIVEL;%s\t%s" % (user_name, 1)
        elif points_student > 3:
            print "LongValueSum:ESTUDANTE;%s\t%s" % (user_name, 1)

def get_key( info, key_search):
    if  len(info.split('=')) == 2:
        (key, value) = info.split('=')
        if key == key_search:
           
            return value
    return None

def main(argv): 

    for line in sys.stdin: 
        text, user_name = None, None
        for info in line.split(';'):
            if not text:
                text = get_key( info, 'text')
            if not user_name:
                user_name = get_key( info, 'user.screen_name')
            if text and user_name:
                break

        if text and user_name:
            filter_tweet( text, user_name )


if __name__ == "__main__": 
    main(sys.argv) 