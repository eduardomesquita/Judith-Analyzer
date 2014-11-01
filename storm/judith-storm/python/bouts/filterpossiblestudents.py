import logging, json, sys

absolute_path = '/'.join( sys.path[0].split('/')[:-1] )
python_libs_path = '/'.join( sys.path[0].split('/')[:-4] )

sys.path.append( absolute_path + '/lib/')
sys.path.append( python_libs_path + '/python-libs/utils/' )
sys.path.append( python_libs_path + '/python-libs/analyzers/filter/' )

import storm_lib as storm
from jsonutils import TwitterJsonUtils
from analyzerpossiblesstudents import AnalyzerPossibleStudentTwitter


class FilterPossibleStudents( storm.BasicBolt ):

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
                analyzer = AnalyzerPossibleStudentTwitter()
                type_of_student = analyzer.filter(  tweet_json  )
                if type_of_student:
                    storm.emit( [ {'type_of_student' : type_of_student, 
                                   'method_name': analyzer.get_name(),
                                   'json' : tweet } ] )
                    
            except Exception as ex:
                storm.emit( [ { 'erro' : '%s;%s'%(ex, tweet_json) } ] )

log = logging.getLogger('processaJson')
log.debug('ProcessaJson loading')
FilterPossibleStudents().run()