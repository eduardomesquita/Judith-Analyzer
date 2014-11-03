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
        