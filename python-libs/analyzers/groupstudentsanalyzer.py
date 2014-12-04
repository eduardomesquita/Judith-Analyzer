import time
from datetime import datetime
from abstract.analyzerabstract import *

class GroupStudentsAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        setattr(self, 'name', 'user_status_count')
  

    def __sum__(self, status_users, user_name, count, status):
        if not status_users.has_key( user_name ):
                status_users[user_name] = {}
        status_users[user_name][status] = count
        return status_users


    def get_raw_data(self, projection = {}):
        status_users = {}
        for bjson in self.analyzer_db.get_raw_data_students():
            user_name, status, count=bjson['userName'],bjson['statusStudents'],bjson['count']
            status_users = self.__sum__(status_users, user_name, count, status)
        
        self.__filter__( **status_users )


    def __filter__(self, **kargs):
        for user in  kargs.keys():

            student, possible = 0, 0
            if kargs[ user ].has_key('POSSIVEL'):
                possible = int(kargs[user]['POSSIVEL'])
            if kargs[ user ].has_key('ESTUDANTE'):
                student = int(kargs[user]['ESTUDANTE'])

            if possible >= student:
                status = 'possible'
            else:
               status = 'student'

            self.find_user_name({ 'statusUsers'  : status,
                                  'totalTweet' : 0,
                                  'userName' : user })

                 
    def find_user_name(self, json_user):
        for cursor in self.twitter_db.get_raw_data_users( user_name=json_user['userName'],
                                                          projection={'_id':0}):
             for bjson in list(cursor):
                 location = bjson['user']['location'] ## ultima localizacao
                 json_user['totalTweet'] += 1

        json_user['name'] = self.name
        self.analyzer_db.save_cache_data( **json_user)  

 
    def init(self):
        print 'inicio cache group students'
        
        self.analyzer_db.remove_cache(name=self.name)
        self.get_raw_data()
        
        print 'fim cache group students'


if __name__ == '__main__':
    GroupStudentsAnalyzer().init()