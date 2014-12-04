from unicodedata import normalize

def remove_non_ascii_chars(text):
   try:
       text = normalize('NFKD', text)
       text = text.encode('ASCII', 'ignore').decode('ASCII')
   except TypeError:
       text = normalize('NFKD', text.decode('UTF-8'))
       text = text.encode('ASCII', 'ignore')

   return text


def get_location(location):
    location  = location.upper()
    if 'PATOS' in location:
        return 'PATOS DE MIMAS'
    elif 'PATIMINAS' in location:
        return 'PATOS DE MIMAS'
    elif 'PRESIDENTE' in location:
        return 'PRESIDENTE OLEGARIO'
    elif 'CARMO' in location:
        return 'CARMO DO PARANAIBA'
    elif 'GOTARDO' in location:
        return 'SAO GOTARDO'
    elif 'VAZANTE' in location:
        return 'VAZANTE'
    elif 'ARAXA' in location:
        return 'ARAXA'
    return None


def replace_word( name):

        if name == 'ADM':
           return 'ADMINISTRACAO'
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
        if name == 'publicidadeepropaganda'.upper():
           return 'PUBLI E PROPAGANDA'
        if name == 'sistemas'.upper():
           return 'SIS INFO'
        if name == 'sistemasdeinformacao'.upper():
           return 'SIS INFO'
        return name

