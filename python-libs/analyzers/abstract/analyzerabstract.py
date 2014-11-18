import sys, pymongo, time
current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
sys.path.append(current_dir + '/python-libs/connectors/mongo/')
sys.path.append(current_dir + '/connectors/mongo/')
from analyzerdb import AnalyzerDB
from twitterdb import TwitterDB
from datetime import datetime
from interfaces.analyzerinterface import AnalyzerInterface


class AnalyzerAbstract(  AnalyzerInterface  ):

    def __init__(self):
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'twitter_db', TwitterDB())
        
    def get_raw_data(self, projection):
        raise NotImplementedError()

    def init(self):
        raise NotImplementedError()

    def get_data_by_key(self, key):
        raise NotImplementedError()