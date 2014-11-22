import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( python_libs_path + '/python-libs/connectors/redis/' )
sys.path.append( python_libs_path + '/python-libs/utils/' )

import storm_lib as storm
from twitterdb import TwitterDB
from jsonutils import TwitterJsonUtils
from redisjudith import RedisJudith
import pymongo

class SaveTwitter( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']

    @classmethod
    def __update_duplicate__(self, tweet):
        redis = RedisJudith()    
        user_name = tweet['user']['screen_name']
        redis.set(name=user_name, value='DUPLICATE' )
        storm.emit( [ {'twetter-status': 'DUPLICATE', 'user' : user_name } ] )
    

    @classmethod
    def process(self, tupla):
        
        tuples = tupla.values[0]

        if not tuples.has_key('erro') and not tuples.has_key('twetter-status') :
   
            tweet = tuples['json']
            method = tuples['method']

            utils = TwitterJsonUtils()
            db = TwitterDB()

            tweet_json = utils.remove_invalid_fields_from_json( tweet )
            try:

                db.save_twitter( tweet_json, method )

                if method == 'by_tags':
                    ## SALVA SOMENTE USERS TWEET BY TAGS
                    storm.emit( [ {'json': tweet, 'response' : 'SALVO' } ] )
                else:
                    storm.emit( [ {'twetter-status': 'BY_USER_NAO_SALVO', 'response' : 'SALVO' } ] )

                  
            except pymongo.errors.DuplicateKeyError as err:
                SaveTwitter.__update_duplicate__( tweet )

            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json), 'CLASS' : 'SaveTwitter'} ] )


log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
SaveTwitter().run()