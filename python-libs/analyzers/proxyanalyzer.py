from studentsanalyzer import *
from tweetanalyzer import *
from configdb import *
from analyzerinterface import AnalyzerInterface
import threading
from analyzerdb import AnalyzerDB


class AnalyzerProxy( AnalyzerInterface ):

    def __init__(self):
        setattr(self,'students_analyzer', StudentsAnalyzer())
        setattr(self,'tweet_analyzer', TweetAnalyzer())
        setattr(self,'config_db', ConfigDB());
        setattr(self, 'analyzer_db', AnalyzerDB())

    def start(self):
        try:
            t1 = threading.Thread(name='Thread-1', target=self.students_analyzer.init)
            t2 = threading.Thread(name='Thread-2', target=self.tweet_analyzer.init)
            t1.start()
            t2.start()
        except Exception as e:
            print "Erro: nao foi possivel iniciar thread. %s" % e
        
    def get_data_by_key(self, key):
        if len(list(self.config_db.find_dust_analyzer())) > 0:
            print 'inicando analize'
            self.start()
            self.config_db.update_dust_analyzer()

        return list(self.analyzer_db.get_cache_analyzer( key ))



        


if __name__ == '__main__':
    a =  AnalyzerProxy()
    print a.get_data_by_key('word_count_status_user')