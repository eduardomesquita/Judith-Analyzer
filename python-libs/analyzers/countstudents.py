import sys, pymongo, time
current_dir =  '/'.join( sys.path[0].split('/')[:-1] )
#sys.path.append(current_dir + '/python-libs/connectors/mongo/')
sys.path.append(current_dir + '/connectors/mongo/')
from analyzerdb import AnalyzerDB
from twitterdb import TwitterDB
from datetime import datetime


class CountStudents(object):

    def __init__(self):
        setattr(self, 'analyzer_db', AnalyzerDB())
        setattr(self, 'twiter_db', TwitterDB())
        
    def __get_raw_data__(self, projection):
        raw_data = {}
        for user_name in self.users.keys():
            raw_data[user_name] = []
            for cursor in self.twiter_db.get_raw_data_users( user_name=user_name,
                                                            projection=projection):
                for bjson in list(cursor):
                    raw_data[user_name].append(bjson)

        return raw_data


    def __get_count_status__(self, **kargs):
        for user in  kargs.keys():
            student, possible = 0, 0
            if kargs[ user ].has_key('POSSIVEL'):
                possible = int(kargs[user]['POSSIVEL'])
            if kargs[ user ].has_key('ESTUDANTE'):
                student = int(kargs[user]['ESTUDANTE'])

            if possible >= student:
                self.users[user] = 'possible'
            else:
                self.users[user] = 'student'       

    def __count_user_status__(self):
        status_users = {}
        for bjson in self.analyzer_db.get_raw_data_students():
            user_name, status, count=bjson['userName'],bjson['statusStudents'],bjson['count']
            if not status_users.has_key( user_name ):
                status_users[user_name] = {}
            status_users[user_name][status] = count
        self.__get_count_status__( **status_users )

    def __aggregation_status__(self):
        for values in self.users.values():
            self.status_users[ values ] += 1

    def __aggregation_location__(self):
        data = self.__get_raw_data__(projection={'user.location':1, '_id':0})
        aggretation = {}
        for user, list_location in data.iteritems():
            aggretation[user] = {}
            for location in list_location:
                city = location['user']['location']
                if city != '':
                    if aggretation[user].has_key( city ):
                        aggretation[user][ city ] += 1
                    else:
                        aggretation[user][ city ] = 1

            if aggretation[user] == {}:
                del aggretation[user]

        self.location = aggretation

    def __aggregation_creat_at__(self):
        data = self.__get_raw_data__(projection={'created_at':1, '_id':0})
        aggretation = {}
        fmt = '%H:%M:%S'
        for user, created_at in data.iteritems():
         
           for str in  created_at:
                ts = time.strftime('%Y-%m-%d %H:%M:%S',
                    time.strptime(str['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
                if aggretation.has_key( ts ):
                    aggretation[ts] += 1
                else:
                    aggretation[ts] = 1

        self.created_at = aggretation

               
    def init(self):

        setattr(self, 'users', {})
        setattr(self, 'status_users', {'possible': 0, 'student':0})
        setattr(self, 'location', {})
        setattr(self, 'created_at', {})

        self.__count_user_status__()
        self.__aggregation_status__()
        self.__aggregation_location__()
        self.__aggregation_creat_at__()
        
        for i in self.created_at:
            print self.created_at[i]


if __name__ == '__main__':
    CountStudents().init()