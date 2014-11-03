import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/utils/' )
sys.path.append( python_libs_path + '/python-libs/connectors/mongo/' )

import storm_lib as storm
from jsonutils import TwitterJsonUtils
from mongojudith import TwitterDB


class SaveTypeOfStudents( storm.BasicBolt ):

    @classmethod
    def declareOutputFields(cls):
        return ['json']

    @classmethod
    def process(self, tupla):
        
        tweet = tupla.values[0]
       
        if tweet.has_key('erro') or tweet.has_key('twetter-status') :
           pass
        else:
            utils = TwitterJsonUtils()
            tweet_json = utils.remove_invalid_fields_from_json( tweet )

            try:

                db = TwitterDB()

                user_name = tweet_json['json']['user']['screen_name']
                id_str = tweet_json['json']['id_str']
                reponse = tweet_json['type_of_student']

                status = db.save_tweet_by_username(user_name=user_name)
                db.insert_judith_metadata(id_str=id_str, status_students=reponse)

                storm.emit([{'status': status }])

            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json) } ] )
            

log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
SaveTypeOfStudents().run()