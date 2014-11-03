#!/usr/bin/python
# -*- coding: utf-8 -*-
from unicodedata import normalize
import re



class FilterTweet( object ):

    def __init__(self):
       
        invalid_users = ['patoshoje', 'CDLPATOS', 'patos_agora', 'portaltopgyn',
                            'oqrola', 'COPASA115', 'g1tvintegracao', 'UNIPAMNET', ]  

        setattr(self,'invalid_users', invalid_users)


    def filter(self, json_analyzer):
        
        user_name = json_analyzer['user']['screen_name']

        if user_name in self.invalid_users:
            return None
        #if 'Minas Gerais' not in json_analyzer['place']['full_name']:
        #    return None

        return True
        
if __name__ == '__main__':

    import  sys, os

    path_python_libs =  '/'.join( sys.path[0].split('/')[:-2] )
    sys.path.append(path_python_libs + '/connectors/mongo/')
    
    from mongojudith import TwitterDB
    
    twitter_db =TwitterDB()

    for data in twitter_db.find_raw_data_users():
        
        f = FilterTweet()
        if f.filter(  data ):
            try:
                user_name = data['user']['screen_name']
                response = twitter_db.save_key_words_by_username( user_name  )
                print response
            except:
                print 'ERRO'