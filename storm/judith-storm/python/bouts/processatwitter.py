import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )

import storm_lib as storm
from mongojudith import TwitterDB

class ProcessaTwitters( storm.BasicBolt ):

    @classmethod
    def __remove_invalid_fields__(self, tweet):
        if isinstance(tweet, dict):
            user_tweet = {}
            for key, values in tweet.iteritems():
                if isinstance(values, dict):
                   user_tweet[ str(key) ]  = self.__remove_invalid_fields__( values )
                elif isinstance(values, unicode):
                    user_tweet[ str(key) ] = values.encode('utf-8')
                elif isinstance(values, list):
                    tmp = []
                    for v in values:
                        if isinstance(v, dict):
                            tmp.append( self.__remove_invalid_fields__(v) ) 
                        else:
                            tmp.append( v )
                    user_tweet[ str(key) ] = tmp
                else:
                    user_tweet[ str(key) ] = values
            return user_tweet
        else:
            return {"TWEET" : str( type(tweet)), "VALOR " :  str( tweet ) }
            
    @classmethod
    def declareOutputFields(cls):
        return ['json_status']

    @classmethod
    def process(self, tupla):
        
        tweet = tupla.values[0]

        if tweet.has_key('erro') or tweet.has_key('twetter-status') :
           storm.emit( [ tweet ] )
        else:
            tweet_json = ProcessaTwitters.__remove_invalid_fields__( tweet )
            try:
                db = TwitterDB()
                db.save_twitter( tweet_json )
                storm.emit( [ {"twetter-status" : "salvo" } ] )
            except Exception as ex:
                storm.emit( [ 'erro:[ %s;%s ]' %  (ex, tweet_json) ] )


log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
ProcessaTwitters().run()