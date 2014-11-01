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
    def save_method_factory(self, tweet_json):
        if tweet_json['method_name'] == 'twitter':
            return TwitterDB()

    @classmethod
    def process(self, tupla):
        
        tweet = tupla.values[0]
       
        if tweet.has_key('erro') or tweet.has_key('twetter-status') :
           pass
        else:
            utils = TwitterJsonUtils()
            tweet_json = utils.remove_invalid_fields_from_json( tweet )

            try:

                judith_db = SaveTypeOfStudents.save_method_factory( tweet_json )

                user_name = tweet_json['json']['user']['screen_name']
                id_str = tweet_json['json']['id_str']
                type_of_student = tweet_json['type_of_student']

                status = judith_db.save_possible_students_tags(user_name=user_name)
                judith_db.update_possible_students_tags(id_str=id_str,
                                                        status_students=type_of_student)
                storm.emit([{'status': status }])

            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json) } ] )
            

log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
SaveTypeOfStudents().run()