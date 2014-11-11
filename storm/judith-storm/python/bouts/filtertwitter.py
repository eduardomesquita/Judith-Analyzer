import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/utils/' )
sys.path.append( python_libs_path + '/python-libs/filter/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )


import storm_lib as storm
from jsonutils import TwitterJsonUtils
from filtertweet import FilterTweet
from twitterdb import TwitterDB

class FilterTwitter( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']

    @classmethod
    def process(self, tupla):
        
        tuples = tupla.values[0]
       
        if tuples.has_key('erro') or tuples.has_key('twetter-status') :
           storm.emit( [ tuples ] )
        else:
            utils = TwitterJsonUtils()
            tweet_json = utils.remove_invalid_fields_from_json( tuples['json'] )
            try:
                filter_ = FilterTweet( TwitterDB() ) 
                status, user_name = filter_.filter( tweet_json  )
                if status is True:
                   storm.emit([ tuples ])
                else:
                    storm.emit( [ { 'erro' : 'NAO_VALIDADO: %s' % user_name } ] )
            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json), 'CLASS' : 'FilterTwitter'}  ] )
            
log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
FilterTwitter().run()