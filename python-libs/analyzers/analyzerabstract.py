import sys, pymongo, time
current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
#sys.path.append(current_dir + '/python-libs/connectors/mongo/')
sys.path.append(current_dir + '/connectors/mongo/')
from analyzerdb import AnalyzerDB
from twitterdb import TwitterDB
from datetime import datetime


class AnalyzerAbstract(object):

    def __init__(self):
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'twiter_db', TwitterDB())

    def get_raw_data(self, projection):
        raise NotImplementedError()

    def init(self):
        raise NotImplementedError()