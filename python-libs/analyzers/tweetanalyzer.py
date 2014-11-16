from analyzerabstract import *
from sets import Set
import time
from datetime import datetime

class TweetAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        setattr(self, 'word_count_all', {})
        setattr(self, 'word_count_status', {})
        setattr(self, 'word_count_all_location', {})
        setattr(self, 'count_status_location', {})

    def get_raw_data(self, projection = {'_id':0}):
        for bjson in self.analyzer_db.get_raw_data_tweets( projection ):
            yield bjson

    def __sort_aggretation__(self, aggretation):
        qtds = aggretation.values()
        qtds.sort()  # ordena as quantidades
        qtds = qtds[-20:] # os 20 maiores
        aggretation_final = {}
        for key, values in aggretation.iteritems():
           if values in qtds:
               aggretation_final[key] = values
        return aggretation_final

    
    def __aggretation_word_count_all_students__(self):

        aggretation = {}
        for tweet in self.get_raw_data( {'_id':0, 'word':1, 'count':1} ):

            word = tweet['word'].upper()
            if len(word)  > 2:
                if aggretation.has_key(word):
                    aggretation[word] += int(tweet['count'])
                else:
                    aggretation[word] = int(tweet['count'])
        self.word_count_all=self.__sort_aggretation__(aggretation)


    def __aggretation_word_count_students__(self):

        aggretation = {}
        for tweet in self.get_raw_data( {'_id':0, 'word':1, 'count':1, 'statusStudents':1} ):
            status_user = tweet['statusStudents']
            if not aggretation.has_key( status_user ) and status_user != 'None':
                aggretation[ status_user ] = {}

            word = tweet['word'].upper()
            if len(word)  > 2 and status_user != 'None':
                if aggretation[ status_user ].has_key(word):
                    aggretation[status_user][word] += int(tweet['count'])
                else:
                    aggretation[status_user][word] = int(tweet['count'])

        for status in aggretation.keys():
            self.word_count_status[status] = self.__sort_aggretation__(aggretation[status])
      
    def __aggretation_status_count_location__(self):

        aggretation = {}
        for tweet in self.get_raw_data( {'_id':0, 'statusStudents':1, 'location':1} ):
            status = tweet['statusStudents'].upper()
            location = tweet['location'].upper()
            if status != 'NONE' and location != 'NONE':
                if aggretation.has_key(status):
                    if aggretation[status].has_key(location):
                       aggretation[status][location] += 1
                    else:
                       aggretation[status][location] = 1
                else:
                    aggretation[status] = {}

        self.count_status_location =  aggretation

    def __aggretation_word_count_location__(self):

        aggretation = {}
        for tweet in self.get_raw_data( {'_id':0, 'word':1, 'count':1, 'location':1} ):
            location = tweet['location']
            if not aggretation.has_key( location ) and location != 'None':
                aggretation[ location ] = {}

            word = tweet['word'].upper()
            if len(word)  > 2 and location != 'None':
                if aggretation[ location ].has_key(word):
                    aggretation[location][word] += int(tweet['count'])
                else:
                    aggretation[location][word] = int(tweet['count'])

        for location in aggretation.keys():
            self.word_count_all_location[location] = self.__sort_aggretation__(aggretation[location])
    

    def init(self):
        self.__aggretation_word_count_all_students__()
        self.__aggretation_word_count_students__()
        self.__aggretation_word_count_location__()
        self.__aggretation_status_count_location__()


if __name__ == '__main__':
    TweetAnalyzer().init()