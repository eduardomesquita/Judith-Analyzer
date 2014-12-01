from pymongo import MongoClient
import json, pymongo
from mongojudith import *
import re


class TwitterDB( MongoJudithAbstract ):

    def __init__(self, ):
        MongoJudithAbstract.__init__( self, db='judith-twitter')


    def default_collection_name(self):
        return 'twittersTags'

    def collection_tweet_users(self):
        return 'twittersUsers'

    def collection_search_tags(self):
        return 'searchTags'

    def collection_search_users(self):
        return 'searchUsers'

    def collection_black_list(self):
        return 'blacklist'



    def __get_collections_search_by_method__(self, method):
        if method == 'by_tags':
            return self.collection_search_tags()
        elif method == 'by_users':
            return self.collection_search_users()

    def __get_collections_tweet_by_method__(self, method):
        if method == 'by_tags':
            return self.default_collection_name()
        elif method == 'by_users':
            return self.collection_tweet_users()



    def get_raw_data_users(self, user_name, projection):
        cursors = []
        for collection_name in [self.default_collection_name(),
                                self.collection_tweet_users()]:
            cursor = self.find_projection( match_criteria={'user.screen_name': user_name},
                                           projection=projection,
                                           collection_name=collection_name)
            cursors.append(cursor)
        return cursors


    def remove_user_name_search(self, user_name):
        self.remove( match_criteria={'keysWords' : [ user_name ]},
                     collection_name=self.collection_search_users() )



    def insert_user_name_in_black_list(self, **kargs):
        self.save( data=kargs,
                   collection_name=self.collection_black_list())

    def find_user_name_black_list(self, user_name):
        return self.find({'username':user_name}, 
                         collection_name=self.collection_black_list())

    def find_all_data_black_list(self):
        return self.find({}, collection_name=self.collection_black_list())

    def remove_black_list(self, **match_criteria):
        return self.remove(match_criteria, collection_name=self.collection_black_list())



    def find_all_search_users(self):
        match_criteria ={'last_tweet_text' : { '$ne' : '' }} 
        return self.find( match_criteria=match_criteria,
                          collection_name=self.collection_search_users())


    def find_tags_search(self, method):
        collection_name = self.__get_collections_search_by_method__(method)  
        for tags in self.find({}, collection_name ):
            yield tags

    def is_new_tweet(self, keys_words, text, method):
        collection_name = self.__get_collections_search_by_method__(method)  
        last_tweet = list(self.find( match_criteria = {'keysWords' : keys_words} , 
                                     collection_name = collection_name))

        if len(last_tweet) > 0:
            return text != last_tweet[0]['last_tweet_text']
        else:
            return True

    def update_last_twitter(self, keys_words, text, method):
        collection_name = self.__get_collections_search_by_method__(method)  
        self.update( match_criteria = {'keysWords' : keys_words },
                     values = {'last_tweet_text' :  text },
                     collection_name = collection_name, upsert = False)

    def update_twitter_uploads_s3(self, id_str, collection_name):
        try:
            self.update( match_criteria = {'id_str' : id_str },
                         values = {'judith-metadata.status' : 'upload_s3' },
                         collection_name = collection_name, upsert = False)
        except:
            raise Exception()
            
    def save_twitter(self, json_twitter, method):
        collection_name = self.__get_collections_tweet_by_method__( method )
        self.save( data = json_twitter, collection_name = collection_name )
            
   
    def find_raw_data_users_paginator(self, collection_name, skip, limit):
        return self.find( {},  collection_name = collection_name).skip( skip ).limit( limit )

    def find_raw_data_users_by_username(self, user_name, collection_name):
        return self.find( {'user.screen_name':user_name}, 
                          collection_name = collection_name)

    def find_raw_data_users(self, user_name):
        return self.find( {'user.screen_name':user_name}, 
                          collection_name=self.collection_tweet_users())

    def find_raw_data_tags(self, user_name):
        return self.find( {'user.screen_name':user_name}, 
                          collection_name=self.default_collection_name())

    def save_key_words_by_username(self, user_name):
        data = {'language' : 'pt', 'keysWords' : [ user_name ], 'last_tweet_text' : '' }
        self.save( data = data, collection_name = self.collection_search_users() )
     
    def save_key_words(self, **kwords):
        kwords['last_tweet_text'] = ''
        self.save(data=kwords,collection_name=self.collection_search_tags())
    
    def get_keys_word(self):
        return list(self.find_projection(match_criteria={},
                                         projection={'_id':0},
                                         collection_name=self.collection_search_tags()))

    def remove_keyswords(self, array):
        tmp = []
        for item in array:
            tmp.append( re.compile('^%s$'% item, re.IGNORECASE) )
            
        match_criteria = {"keysWords" : {'$all' : tmp} }
        self.remove( match_criteria=match_criteria,
                    collection_name=self.collection_search_tags())