import time
from datetime import datetime
from analyzerabstract import *

class StudentsAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        setattr(self, 'users', {})
        setattr(self, 'status_users_count', {'possible': 0, 'student':0})
        setattr(self, 'location', {})
        setattr(self, 'created_at', {})
        setattr(self, 'raw_data', {})

    def get_raw_data(self, projection):
        self.raw_data = {}
        for user_name in self.users_status_name.keys()[:5]:

            self.raw_data[user_name] = []
            for cursor in self.twiter_db.get_raw_data_users( user_name=user_name,
                                                             projection=projection):
                for bjson in list(cursor):
                    self.raw_data[user_name].append(bjson)


    def sum_dict(self, key, **kargs):
        
        if kargs.has_key( key ):
            kargs[ key ] += 1
        else:
            kargs[ key ] = 1
        return kargs

    def __get_count_status__(self, **kargs):
        for user in  kargs.keys():
            student, possible = 0, 0
            if kargs[ user ].has_key('POSSIVEL'):
                possible = int(kargs[user]['POSSIVEL'])
            if kargs[ user ].has_key('ESTUDANTE'):
                student = int(kargs[user]['ESTUDANTE'])

            if possible >= student:
                self.users_status_name[user] = 'possible'
            else:
                self.users_status_name[user] = 'student'


    def __count_user_status__(self):
        status_users = {}
        for bjson in self.analyzer_db.get_raw_data_students():
            user_name, status, count=bjson['userName'],bjson['statusStudents'],bjson['count']
            if not status_users.has_key( user_name ):
                status_users[user_name] = {}
            status_users[user_name][status] = count

        self.__get_count_status__( **status_users )

    def __aggregation_status__(self):
        for key, values in self.users_status_name.iteritems():
            self.status_users_count[ values ] += 1

    def __aggregation_location__(self):

        aggretation = {}
        for user, list_bjson in self.raw_data.iteritems():
            aggretation[user] = { 'location': {},
                                  'status_users' : self.users_status_name[ user ] }
            for bjson in list_bjson:
                if bjson['user'].has_key('location'):
                    location = bjson['user']['location']          
                    if location != '':
                        aggretation[user]['location'] = self.sum_dict(location, **aggretation[user]['location'])
            
            if aggretation[user]['location'] == {}:
                del aggretation[user]
        
        self.location = aggretation

    def __aggregation_creat_at__(self, fmt = '%H:%M:%S'):
     
        aggretation = {}
        for user, list_bjson in self.raw_data.iteritems():
            aggretation[user] = { 'created_tweet_at': {'year':{},
                                                       'month':{},
                                                       'day':{},
                                                       'hour':{},
                                                       'minute':{}},
                                  'status_users' : self.users_status_name[ user ] }

            for bjson in list_bjson:
               if bjson.has_key('created_at'):
                    ts = time.strftime('%Y-%m-%d %H:%M:%S',
                        time.strptime(bjson['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))

                    (data_str, time_str) = ts.split(' ')
                    (y, m, d) = data_str.split('-')
                    (h, m, s) = time_str.split(':')

                    aggretation[user]['created_tweet_at']['year'] = self.sum_dict(y, **aggretation[user]['created_tweet_at']['year'])
                    aggretation[user]['created_tweet_at']['month'] = self.sum_dict(m, **aggretation[user]['created_tweet_at']['month'])
                    aggretation[user]['created_tweet_at']['day'] = self.sum_dict(d, **aggretation[user]['created_tweet_at']['day'])
                    aggretation[user]['created_tweet_at']['hour'] = self.sum_dict(h, **aggretation[user]['created_tweet_at']['hour'])
                    aggretation[user]['created_tweet_at']['minute'] = self.sum_dict(s, **aggretation[user]['created_tweet_at']['minute'])

        self.created_at = aggretation

               
    def init(self):

        #print 'start cache..'

        self.users_status_name =  {}
        self.__count_user_status__()
        self.analyzer_db.save_cache_data('users_status_name', **self.users_status_name ) 

        self.status_users_count =  {'possible': 0, 'student':0}
        self.__aggregation_status__()
        self.analyzer_db.save_cache_data('user_status_count', **self.status_users_count ) 

        self.get_raw_data({'_id':0})

        self.location = {}
        self.__aggregation_location__()
        self.analyzer_db.save_cache_data('user_status_location', **self.location ) 
        
        self.created_at = {}
        self.__aggregation_creat_at__()
        self.analyzer_db.save_cache_data('user_status_created_at', **self.created_at ) 

        #print 'Fim cache..'

if __name__ == '__main__':
    StudentsAnalyzer().init()