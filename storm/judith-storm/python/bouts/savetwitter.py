import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )
sys.path.append( python_libs_path + '/python-libs/connectors/redis/' )
sys.path.append( python_libs_path + '/python-libs/utils/' )

import storm_lib as storm
from mongojudith import TwitterDB
from jsonutils import TwitterJsonUtils
from redisjudith import RedisJudith

class ProcessaTwitters( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']

    @classmethod
    def process(self, tupla):
        
        tuples = tupla.values[0]

        if tuples.has_key('erro') or tuples.has_key('twetter-status') :
           storm.emit( [ tuples ] )
        else:

            tweet = tuples['json']
            method = tuples['method']

            utils = TwitterJsonUtils()
            tweet_json = utils.remove_invalid_fields_from_json( tweet )

            try:
                db = TwitterDB()
                response = db.save_twitter( tweet_json, method)
                user_name = tweet['user']['screen_name']

                if response == 'tweet_save':
                    if method == 'by_tags':
                        storm.emit( [ tweet ] )
                    elif method == 'by_users':
                        storm.emit( [ { 'twetter-status' : 'save_by_users: %s' % (user_name)} ])
                else:
                    ''' salva usename para envitar testar duplicados ''' 
                    redis = RedisJudith()
                    redis.set(name=user_name, value='DUPLICATE' )
                    storm.emit( [ { 'erro' : 'duplicate %s' % (user_name) } ])

            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json) } ] )



log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
ProcessaTwitters().run()