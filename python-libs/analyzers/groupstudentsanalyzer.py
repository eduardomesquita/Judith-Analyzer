import time
from datetime import datetime
from abstract.analyzerabstract import *

class GroupStudentsAnalyzer(  AnalyzerAbstract ):

    def __init__(self):
        AnalyzerAbstract.__init__(self)
        setattr(self,'users_status_name', [])

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
            
            self.users_status_name.append({ 'statusUsers'  : status,
                                            'totalTweet' : 0,
                                            'userName' : user })

                 
    def find_user_name(self):
        for json_user in self.users_status_name:
            for cursor in self.twitter_db.get_raw_data_users( user_name=json_user['userName'],
                                                              projection={'_id':0}):
                 for bjson in list(cursor):
                     location = bjson['user']['location']
                     json_user = self.emit( json_user, location, 1)

    def emit(self, json_user, location, count_tweet):
        json_user['location'] = location
        json_user['totalTweet'] += int(count_tweet)
        return json_user
      

    def init(self):
        print 'inicio cache group students'
        
        self.get_raw_data()
        self.find_user_name()

        for json_user in self.users_status_name:

            if json_user['totalTweet'] > 0:
                self.analyzer_db.save_students_count_tweet( **json_user )

        print 'fim cache group students'


if __name__ == '__main__':
    GroupStudentsAnalyzer().init()