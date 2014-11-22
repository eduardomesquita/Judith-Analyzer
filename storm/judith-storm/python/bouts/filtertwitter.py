import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/utils/' )
sys.path.append( python_libs_path + '/python-libs/filter/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( python_libs_path + '/python-libs/connectors/redis/' )

import storm_lib as storm
from jsonutils import TwitterJsonUtils
from filtertweet import FilterTweet
from twitterdb import TwitterDB
from redisjudith import RedisJudith

class FilterTwitter( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']

    @classmethod
    def __update_duplicate__(self, tweet):
        redis = RedisJudith()    
        user_name = tweet['user']['screen_name']
        redis.set(name=user_name, value='BACKLIST' )
        storm.emit( [ {'twetter-status': 'BACKLIST', 'user' : user_name } ] )


    @classmethod
    def process(self, tupla):
        
        tuples = tupla.values[0]
       
        if not tuples.has_key('erro') and not tuples.has_key('twetter-status') :
            
            utils = TwitterJsonUtils()
            filter_ = FilterTweet( TwitterDB() ) 

            tweet = utils.remove_invalid_fields_from_json( tuples['json'] )

            try:
                
                status, user_name = filter_.filter( tweet )

                if status is True:
                   storm.emit([ tuples ])
                else:
                    FilterTwitter.__update_duplicate__( tweet )
            
            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet), 'CLASS' : 'FilterTwitter'}  ] )
            


log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
FilterTwitter().run()