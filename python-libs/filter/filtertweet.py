#!/usr/bin/python
import re

class FilterTweet( object ):

    def __init__(self, twitter_db):
        setattr(self, 'twitter_db', twitter_db)

    def __find_user_name_in_blacklist__(self, user_name):
        result_set = list(self.twitter_db.find_user_name_black_list( user_name ))
        return len(result_set) > 0

    def filter(self, json_analyzer):
        
        user_name = json_analyzer['user']['screen_name']
        if self.__find_user_name_in_blacklist__( user_name ):
            return False, user_name

        return True, user_name
