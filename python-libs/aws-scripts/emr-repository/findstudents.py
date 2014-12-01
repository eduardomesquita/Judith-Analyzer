#!/usr/bin/python
import re, sys


possible_student_dictionary = { 'vestibular' : 5, 'unipam': 5,
                                'matriculada' : 4, 'matriculado' : 4,
                                'matricular' : 4, 'matricula' : 4,
                                'aprovada' : 4, 'aprovado' : 4,
                                'passei' : 2,  'resultado'  : 2,
                                'passado' : 2, 
                                'prova' : 3, 'vest' : 5,
                                'passar': 3, 'inscricao' : 3,
                                'curso'  : 2,'enem' : 7, 'fazer' : 2,
                                'colegio' : 2,'normal' : 1,
                                'marcolino' : 5,'fpm' : 2,
                                'faculdade' : 4, 'facul' : 4,'zama' : 1,
                                'maciel' : 1, 'patos de minas' : 1,
                                'patosdeminas' : 1, 'patos' : 1,
                                'bloco' : 2, 'comprovante' : 3,
                                'fazer' : 3,'turma':3, 'abrir' : 1,
                                '2015' : 3,'administracao' : 3,
                                'agronegocio' : 3, 'agronomia' : 3,
                                'arquitetura' : 3, 'urbanismo' : 3,
                                'ciencias' : 3,'biologicas' : 3,
                                'cienciasbiologicas' : 3,'biologia' : 3,
                                'cienciascontabeis' : 3,'contabeis' : 3,
                                'direito' : 3,'adm' : 3,
                                'educacao fisica' : 3,'educacaofisica' : 3,
                                'enfermagem' : 3,'engenharia' : 3,
                                'ambiental' : 3,'engenhariaambiental' : 3,
                                'engenharia' : 3,'engenhariacivil' : 3,
                                'engenharia' : 3,'eletrica' : 3,
                                'engenhariaeletrica' : 3,'engenharia' : 3,
                                'mecanica' : 3,'engenhariamecanica' : 3,
                                'engenharia' : 3,'producao' : 3,
                                'engenhariadeproducao' : 3,'engenhariaproducao' : 3,
                                'engenharia quimica' : 3,'engenhariaquimica' : 3,
                                'farmacia' : 3,'fisioterapia' : 3,
                                'gestao comercial' : 3, 'gestaocomercial' : 3,
                                'historia' : 3, 'jornalismo' : 3,
                                'Letras' : 3,'medicina' : 3,'nutricao' : 3,
                                'pedagogia' : 3,'psicologia' : 3,
                                'publicidade' : 3,'publicidadeepropaganda' : 3,
                                'publicidade' : 3,'propaganda' : 3,
                                'sistemas' : 3,'informacao' : 3,
                                'sistemasdeinformacao' : 3,'sistemas' : 3,
                                'zootecnia' : 3
                               }

student_dictionary = { 
                        'unipam': 5, 'fepam' : 5,
                        'comine': 8, 'bloco'  : 5,
                        'faculdade' : 4, 'facul' : 4,
                        'portal' : 2, 'aula' : 3,
                        'palestra':2, 'administracao' : 3,
                        'agronegocio' : 3, 'agronomia' : 3,
                        'arquitetura' : 3, 'urbanismo' : 3,
                        'ciencias' : 3,'biologicas' : 3,
                        'cienciasbiologicas' : 3,'biologia' : 3,
                        'cienciascontabeis' : 3,'contabeis' : 3,
                        'direito' : 3,'adm' : 3,
                        'educacao fisica' : 3,'educacaofisica' : 3,
                        'enfermagem' : 3,'engenharia' : 3,
                        'ambiental' : 3,'engenhariaambiental' : 3,
                        'engenharia' : 3,'engenhariacivil' : 3,
                        'engenharia' : 3,'eletrica' : 3,
                        'engenhariaeletrica' : 3,'engenharia' : 3,
                        'mecanica' : 3,'engenhariamecanica' : 3,
                        'engenharia' : 3,'producao' : 3,
                        'engenhariadeproducao' : 3,'engenhariaproducao' : 3,
                        'engenharia quimica' : 3,'engenhariaquimica' : 3,
                        'farmacia' : 3,'fisioterapia' : 3,
                        'gestao comercial' : 3, 'gestaocomercial' : 3,
                        'historia' : 3, 'jornalismo' : 3,
                        'Letras' : 3,'medicina' : 3,'nutricao' : 3,
                        'pedagogia' : 3,'psicologia' : 3,
                        'publicidade' : 3,'publicidadeepropaganda' : 3,
                        'publicidade' : 3,'propaganda' : 3,
                        'sistemas' : 3,'informacao' : 3,
                        'sistemasdeinformacao' : 3,'sistemas' : 3,
                        'zootecnia' : 3,'patos de minas' : 1,
                        'patosdeminas' : 1, 'patos' : 1,
                        'curso' : 2,'xicomine' : 5, 'unipamnet' : 5,
                        'avin' : 8, 'JUU' : 5, 'resultado'  : 1,
                        'semestre'  : 4,
                    }


def points( word ):
    word = word.lower().split('#')
    pattern = re.compile("[a-z|A-Z]")
    p1, p2 = 0, 0

    for case in word:
        case = "".join(re.findall(pattern, case))
        if possible_student_dictionary.has_key( case ):
           p1 =  possible_student_dictionary[ case ]

        if student_dictionary.has_key( case  ):
           p2 = student_dictionary[ case ]

    return p1, p2


def filter_tweet(text, user_name):
    all_words = {}
    points_possible_student = 0 
    points_student = 0
    pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*") 

    for word in pattern.findall(text):
        word = word.lower()
        if not all_words.has_key( word ):
            all_words[word] = 1
            (p1, p2) = points( word=word )
                
            points_possible_student  += p1
            points_student  += p2

    if points_possible_student > 5 and points_possible_student > points_student:
        print "LongValueSum:POSSIVEL;%s\t%s" % (user_name, 1)
    elif points_student > 4:
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