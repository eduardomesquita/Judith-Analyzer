from pymongo import MongoClient
import json

class MongoJudith(object):

    def __init__(self, host = 'localhost', port = 27017 , db  = 'judith-project'):
       setattr(self,'client', MongoClient( host , port ) )
       setattr(self,'mongo_db', self.client[ db ])

    def default_collection_name(self):
        raise NotImplementedError()

    def save(self, json_save, collection_name):
        self.mongo_db[ collection_name ].save( json_save )

    def update(self, key, values, collection_name, upsert = True):
        self.mongo_db[ collection_name ].update( key, {'$set': values}, upsert = upsert)

    def find(self, json_find, collection_name):
        return self.mongo_db[ collection_name ].find( json_find )



class TwitterDB( MongoJudith ):

    def __init__(self, ):
        MongoJudith.__init__( self, db='judith-twitter')

    def default_collection_name(self):
        return 'twitters'

    def __default_collection_search__(self):
        return 'searchTags'

    def find_tags_search(self):
        for tags in self.find({}, self.__default_collection_search__() ):
            yield tags

    def save_twitter(self, json_twitter):
        self.save( json_save = json_twitter, collection_name = self.default_collection_name() )

    def is_new_tweet(self, keys_words, text):
        last_tweet = list(self.find( json_find = {'keysWords' : keys_words} , 
                                     collection_name = self.__default_collection_search__() ))
        if len(last_tweet) > 0:
            return text != last_tweet[0]['last_tweet_text']
        else:
            return True

    def save_last_twitter(self, keys_words, text):
        self.update( key = {'keysWords' : keys_words },
                     values = {'last_tweet_text' :  text },
                     collection_name = self.__default_collection_search__(), upsert = False)

if __name__ == '__main__':
    json = {"lang": "pt", "text": "vou fazer o enem no bloco f da unipam", "created_at": "Wed Oct 29 00:19:58 +0000 2014", "metadata": {"iso_language_code": "pt", "result_type": "recent"}, "source": "<a href=\"http://twitter.com\" rel=\"nofollow\">Twitter Web Client</a>", "id_str": "527253471939289088", "retweet_count": 0, "id": 527253471939289088, "favorite_count": 0, "user": {"profile_use_background_image": 1, "profile_background_image_url_https": "https://pbs.twimg.com/profile_background_images/526108235644669952/ND8XjDk7.jpeg", "profile_text_color": "333333", "profile_image_url_https": "https://pbs.twimg.com/profile_images/516357311086858241/OpSAq5OX_normal.jpeg", "profile_sidebar_fill_color": "DDEEF6", "id": 1325326586, "entities": {"description": {"urls": []}}, "followers_count": 721, "location": "Patos de Minas", "profile_background_color": "EB114E", "id_str": "1325326586", "utc_offset": -7200, "statuses_count": 14590, "description": "A vida me fez Galo, e eu fiz do Galo minha vida \u2665", "friends_count": 677, "profile_link_color": "050D0F", "profile_image_url": "http://pbs.twimg.com/profile_images/516357311086858241/OpSAq5OX_normal.jpeg", "geo_enabled": 1, "profile_banner_url": "https://pbs.twimg.com/profile_banners/1325326586/1413848173", "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/526108235644669952/ND8XjDk7.jpeg", "screen_name": "mds_gabi", "lang": "pt", "profile_background_tile": 1, "favourites_count": 2798, "name": " Gabi ", "created_at": "Wed Apr 03 20:06:18 +0000 2013", "time_zone": "Brasilia", "profile_sidebar_border_color": "FFFFFF", "listed_count": 2}}
    t = TwitterDB()
    t.save_twitter( json )