import logging, sys, json, time

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( python_libs_path + '/python-libs/twitter/twitter-api/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( python_libs_path + '/python-libs/connectors/redis/' )
sys.path.append( absolute_path + '/lib/')

import storm_lib as storm
from twitterfeed import TwitterApi
from twitterdb import TwitterDB
from redisjudith import RedisJudith

class TwitterSpout(storm.Spout):


    @classmethod
    def __update_last_tweet__(self, twitter_db, keysWords, content, method):
        twitter_db.update_last_twitter( keysWords, content, method)

    @classmethod
    def __check_is_new_twitter__(self, search, twitter_db, method):

        try:

            tweets = []
            content = None
            twitter_api = TwitterApi()

            generator_tweet = twitter_api.find_hashtags( keys_words = search['keysWords'],
                                                         language = search['language'],
                                                         qtd_page = 100 )
            content = generator_tweet.next()
            new_tweet = twitter_db.is_new_tweet( search['keysWords'], content['text'], method )

            if new_tweet  is True:
                TwitterSpout.__update_last_tweet__( twitter_db=twitter_db,
                                                    keysWords=search['keysWords'],
                                                    content=content['text'],
                                                    method=method)
                while content:
                    tweets.append( content )
                    content = generator_tweet.next()

        except StopIteration as ex:
            pass
        return tweets


    @classmethod
    def __clear_duplicate__(self, news_tweets, redis):
        for tweet_json in news_tweets:
            user_name = tweet_json['user']['screen_name']
            redis.delete(name=user_name)

    @classmethod
    def __check_duplicate_tweet__(self, redis, user_name):

        status_redis = redis.get(name=user_name)
        if status_redis is None:
            return True 
        return False

    @classmethod
    def __emit_tweets__(self, news_tweets, redis, search, method):
        for tweet_json in news_tweets:
            user_name = tweet_json['user']['screen_name']
            duplicate = TwitterSpout.__check_duplicate_tweet__( redis=redis,
                                                                user_name=user_name )
            if duplicate is True:
                storm.emit( [ {'KEYWORDS' : search['keysWords'], 'json': tweet_json,  'method': method }] )
                time.sleep(1)
            else:
                storm.emit( [ { 'twetter-status' : 'DUPLICATE', 'keys_words' : search['keysWords'] }] )
                break


    @classmethod
    def __find_tweets__(self, twitter_db, method):

        redis = RedisJudith()
        tags_generator = twitter_db.find_tags_search( method )

        for search in tags_generator:
            news_tweets = TwitterSpout.__check_is_new_twitter__( search=search,
                                                                 twitter_db=twitter_db,
                                                                 method=method )
            if len(news_tweets) > 0:

                TwitterSpout.__clear_duplicate__( news_tweets=news_tweets,
                                                  redis=redis )

                TwitterSpout.__emit_tweets__( news_tweets=news_tweets, 
                                              redis=redis, search=search,
                                              method=method)
            else:
                storm.emit( [ { 'twetter-status' : 'sem atualizacao', 'keys_words' : search['keysWords'] }] )

            time.sleep(60)

    @classmethod
    def nextTuple(self):
        try:
            
            twitter_db = TwitterDB()
            for method in ['by_tags','by_users']:
                TwitterSpout.__find_tweets__( twitter_db=twitter_db, method=method )
                
        except Exception as ex:
            storm.emit( [ { 'erro' : '%s' % ex , 'CLASS' : 'FilterTwitter'}] )
            time.sleep( 60 )
            
        time.sleep( 600 )


if __name__ == '__main__':
    log = logging.getLogger('TwitterSpout')
    log.debug('TwitterSpout loading...')
    TwitterSpout().run()