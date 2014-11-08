import logging, sys, json, time

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( python_libs_path + '/python-libs/twitter/twitter-api/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( python_libs_path + '/python-libs/connectors/redis/' )
sys.path.append( absolute_path + '/lib/')

import storm_lib as storm
from twitterfeed import TwitterApi
from mongojudith import TwitterDB
from redisjudith import RedisJudith

class TwitterSpout(storm.Spout):

    @classmethod
    def __check_is_new_twitter__(self, search, twitter_db, method):

        tweets = []
        content = None
        twitter_api = TwitterApi()
        try:
            
            generator_tweet = twitter_api.find_hashtags( keys_words = search['keysWords'],
                                                         language = search['language'],
                                                         qtd_page = 100 )
            content = generator_tweet.next()

            if twitter_db.is_new_tweet( search['keysWords'], content['text'], method ) is True:
                twitter_db.update_last_twitter( search['keysWords'], content['text'], method)

                while content:
                    tweets.append( content )
                    content = generator_tweet.next()

        except StopIteration as ex:
            pass

        return tweets

    @classmethod
    def __check_duplicate_tweet__(self, redis, user_name, start):
        if start is True:
            redis.delete(name=user_name)
            return True
        else:
            status_redis = redis.get(name=user_name)
            if status_redis is None:
                return True
            elif status_redis == 'DUPLICATE':
                return False
            else:
                return True

    @classmethod
    def __find_tweets__(self, twitter_db, method):

        redis = RedisJudith()
        tags_generator = twitter_db.find_tags_search( method )

        for search in tags_generator:
            tweet_iter = TwitterSpout.__check_is_new_twitter__( search,twitter_db,
                                                                method)
            if len(tweet_iter) > 0:
                start = True

                for tweet_json in tweet_iter:
                    user_name = tweet_json['user']['screen_name']

                    check_duplicate = self.__check_duplicate_tweet__( redis, user_name,
                                                                      start)
                    if check_duplicate is True:
                        storm.emit( [ {'json': tweet_json,
                                       'method': method }] )
                        time.sleep(1)
                        start = False
                    else:
                        storm.emit( [ { 'twetter-status' : 'duplicate',
                                        'keys_words' : search['keysWords'] }] )
            else:
                storm.emit( [ { 'twetter-status' : 'sem atualizacao',
                                'keys_words' : search['keysWords'] }] )
            time.sleep(60)


    @classmethod
    def nextTuple(self):
        try:

            twitter_db = TwitterDB()
            
            for method in ['by_tags','by_users']:
                tweet_iter = TwitterSpout.__find_tweets__( twitter_db, 
                                                           method )
            time.sleep( 6000 )
        except Exception as ex:
            storm.emit( [ { 'erro' : '%s' % ex , 'CLASS' : 'FilterTwitter'}] )
            time.sleep( 60 )

if __name__ == '__main__':
    
    log = logging.getLogger('TwitterSpout')
    log.debug('TwitterSpout loading...')
    TwitterSpout().run()
    #TwitterSpout.nextTuple()