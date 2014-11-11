import sys, pymongo

path_python_libs =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(path_python_libs + '/python-libs/connectors/mongo/')
sys.path.append(path_python_libs + '/python-libs/aws-api/')
from analyzerdb import AnalyzerDB
from twitterdb import TwitterDB

class BlackListTweet(object):

    def __init__(self):
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'twitter_db', TwitterDB())

    def __remove_search_users__(self, user_name):
        print 'removendo da busca %s' % user_name
        self.twitter_db.remove_user_name_search( user_name=user_name )

    def __insert_into_blacklist__(self, **kargs):
        for search in list(self.twitter_db.find_all_search_users()):
            print search['keysWords'][0]
            if search['keysWords'][0] not in kargs.keys():
                data = {'username' : search['keysWords'][0]}
                try:
                    print 'inserindo na black_list %s' % search['keysWords'][0]
                    self.twitter_db.insert_user_name_in_black_list( **data )
                    self.__remove_search_users__( user_name=search['keysWords'][0] )
                except pymongo.errors.DuplicateKeyError  as e:
                     print e

    def add_black_list(self):
        filter_duplicate = {}
        for students in list(self.analyzer_db.get_raw_data_students()):
            filter_duplicate[ students['userName'] ] = 1
        self.__insert_into_blacklist__( **filter_duplicate )
      

#invalid_users = ['patoshoje', 'CDLPATOS', 'patos_agora', 'portaltopgyn','oqrola', 'COPASA115', 'g1tvintegracao', 'UNIPAMNET', ] 

if __name__ == '__main__':
    black_list = BlackListTweet()
    black_list.add_black_list()