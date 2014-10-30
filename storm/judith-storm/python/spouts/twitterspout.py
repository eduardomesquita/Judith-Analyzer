import logging, sys, json, time

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( python_libs_path + '/python-libs/twitter/twitter-api/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( absolute_path + '/lib/')
import storm_lib as storm
from twitterfeed import TwitterApi
from mongojudith import TwitterDB

class TwitterSpout(storm.Spout):


    @classmethod
    def __check_is_new_twitter__(self, generator_tweet, keys_words, twitter_db):
        tweets = []
        try:
            
            content = generator_tweet.next()

            if twitter_db.is_new_tweet( keys_words, content['text'] ) is True:
                twitter_db.save_last_twitter( keys_words, content['text'] )
                while content:
                    try:
                        tweets.append( content )
                        content = generator_tweet.next()
                    except StopIteration as e:
                       break

        except Exception as ex:
            raise Exception('origin: __check_is_new_twitter__ : %s ' % str(ex))
        return tweets


    @classmethod
    def nextTuple(self):
        try:
            
            twitter_db = TwitterDB()
            twitter_api = TwitterApi()

            tags_generator = twitter_db.find_tags_search()
            for search in tags_generator:
                print search
                contents_generator_tweet = twitter_api.find_hashtags( keys_words = search['keysWords'],
                                                                      language = search['language'],
                                                                      qtd_page = 1 )

                twitter_iter = TwitterSpout.__check_is_new_twitter__( contents_generator_tweet, 
                                                                      search['keysWords'], twitter_db )
                print len(twitter_iter)
                if len(twitter_iter) > 0:
                    for twitter_json in twitter_iter:
                        print twitter_json['text']

                        #storm.emit( [ twitter_json ] )
                        #time.sleep( 1 )
                else:
                    print 'mesmo tweet'
                    #storm.emit( [ { 'twetter-status' : 'sem atualizacao' } ] )
                    pass
                print '\n\n'
                time.sleep(60)
              
        except Exception as ex:
               print ex
               #storm.emit( [ { 'erro' : '%s' % ex} ] )
               pass

        time.sleep( 300 )

if __name__ == '__main__':
    
    log = logging.getLogger('TwitterSpout')
    log.debug('TwitterSpout loading...')
    #TwitterSpout().run()
    TwitterSpout.nextTuple()