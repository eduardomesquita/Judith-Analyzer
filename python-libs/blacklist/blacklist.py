import sys, pymongo

current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(current_dir + '/connectors/mongo/')
sys.path.append(current_dir + '/aws-api/')
sys.path.append(current_dir + '/utils/')
from analyzerdb import AnalyzerDB
from twitterdb import TwitterDB
import dateutils as date_utils
from configdb import ConfigDB
import dateutils as date_utils

class BlackListTweet(object):

    def __init__(self):
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'twitter_db', TwitterDB())
        setattr(self, 'config_db', ConfigDB())

    def __remove_search_users__(self, user_name):
        print 'nao irei pesquisar mais por: %s' % user_name
        self.twitter_db.remove_user_name_search( user_name=user_name )

    def __insert_into_blacklist__(self, **kargs):

        count = 0
        for search in list(self.twitter_db.find_all_search_users()):
            print 'pesquisando para remove da blacklist twitter: %s'  % (''.join(search['keysWords']))

            if search['keysWords'][0] not in kargs.keys():  ## todos que nao forem studantes 
                data = {'username' : search['keysWords'][0]}
                data['created_at'] = date_utils.current_time()
                try:
                    self.twitter_db.insert_user_name_in_black_list( **data )
                    self.__remove_search_users__( user_name=search['keysWords'][0] )
                    count += 1
                except pymongo.errors.DuplicateKeyError  as e:
                    print e

        self.save_logs_blacklist(text='Dados inseridos na blacklist twitter %s' % count)

    def add_black_list(self):
        remove_duplicate = {}
        for students in list(self.analyzer_db.get_raw_data_students()):
            remove_duplicate[ students['userName'] ] = 1

        self.__insert_into_blacklist__( **remove_duplicate )

    def save_logs_blacklist(self, text):
        self.config_db.save_log( **{'text':text, 'date': date_utils.current_time(), 'type' : 'BLACKLIST', 'scriptName'  : 'BLACKLIST-TWITTER'} )



if __name__ == '__main__':
    BlackListTweet().add_black_list()
