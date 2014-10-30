import twitter, json
from TwitterSearch import *

USER_AUTH = { 'consumer_key' : 'WDm1ZyIL4JudaJLKw0JsWbjAR',
              'consumer_secret' : 'Bqhn60ogrlLh36gxWPuvG5bn2bJGQy03RVAfnwSizUTsBgiuPb',
              'access_token' : '2824866182-H03CoYXueJvdcrjGmkVHmzo0Ma9a3Zg1Y9aQ0yE',
              'access_token_secret' : 'XgzE8arwulctpyciHsIGYUcCGpIWBAc3JfIlVUkqgbHLK'
}

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
    hash_tags = ['unipam']
    generators = twitter.find_hashtags( hash_tags,'pt', 1 )

    valor = generators.next()
    while  valor:
        try :
            print valor['text']
            valor = generators.next()
        except StopIteration as e:
             break 
