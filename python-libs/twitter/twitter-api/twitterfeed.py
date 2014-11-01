import twitter, json, sys, platform
from TwitterSearch import *


def read_credential():
    
    if  platform.node() == 'eduardo-linux':
        PATH = '/home/eduardo/Projetos/TCC/keys/twitter_keys.csv'
    else:
        project_path = '/'.join( sys.path[0].split('/')[:-4] )
        PATH = project_path + '../../../keys/twitter_keys.csv'

    return dict( [  line.strip().split('=') 
                            for line in open( PATH, 'r')] ) 

USER_AUTH = read_credential()

class TwitterApi(object):

    def __init__(self):
        setattr(self, 'search', self.__is_authentication__( **USER_AUTH ))
        setattr(self, 'config', TwitterSearchOrder())

    def __is_authentication__(self, **kwargs):
        ts = TwitterSearch( **USER_AUTH )
        return ts

    def __search_twitters__(self):
       for tweet in self.search.searchTweetsIterable( self.config ):
            yield tweet
           
    def find_hashtags(self, keys_words, language, qtd_page):
        try:
            self.config.setKeywords( keys_words )
            self.config.setLanguage( language )
            self.config.setCount( qtd_page )
            self.config.setIncludeEntities(False)
            self.config.setResultType('recent')
            return self.__search_twitters__()
        except Exception as ex:
            raise Exception('origin: find_hashtags : %s ' % str(ex))

if __name__ == '__main__':

    twitter = TwitterApi()
    hash_tags = ['patosdeminas']
    generators = twitter.find_hashtags( hash_tags,'pt', 1 )

    valor = generators.next()
    while  valor:
        try :
            print valor['text']
            print '\n'
            valor = generators.next()
        except StopIteration as e:
             break 
