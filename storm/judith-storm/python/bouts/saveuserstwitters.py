import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/utils/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )

import storm_lib as storm
from jsonutils import TwitterJsonUtils
from twitterdb import TwitterDB
import pymongo

class SaveUsersTwitters( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']
        
    @classmethod
    def process(self, tupla):
        
        tweet = tupla.values[0]
        if not tweet.has_key('erro') and not tweet.has_key('twetter-status') :

            utils = TwitterJsonUtils()
            db = TwitterDB()

            tweet_json = utils.remove_invalid_fields_from_json( tweet['json'] )

            try:
               
                user_name = tweet_json['user']['screen_name']
                db.save_key_words_by_username( user_name  )
                storm.emit( [ {'twetter-status-urser' : user_name ,'response': 'SALVO'} ] )

            except pymongo.errors.DuplicateKeyError as err:
                pass

            except Exception as ex:
               storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json), 'CLASS' : 'SaveUsersTwitters' } ] )



log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
SaveUsersTwitters().run()