from threading import Thread
from datetime import date
import datetime, sys, time, re
current_dir =  '/'.join( sys.path[0].split('/')[:-1])
sys.path.append(current_dir + '/python-libs/aws-scripts/s3-uploads/')
sys.path.append(current_dir + '/python-libs/aws-scripts/emr-crontoller/')
sys.path.append(current_dir + '/python-libs/aws-scripts/utils/')
sys.path.append(current_dir + '/python-libs/blacklist/')


import dateutils as date_utils
from uploadalltweetsS3 import AwsS3AllTweetUpload
from uploadtweetofstudens import AwsS3TweetsOfStudens
from emrfilterstudents import EmrFilterStudents
from emrtweetcount import EmrTweetCount
from blacklist import BlackListTweet


class JobsBlackList( object ):

    def __init__(self, name):
        setattr(self,'blacklist', BlackListTweet())
        setattr(self, 'name', name)


    def set_status(self, status, config_db):
        config_db.update_aws_script_blacklist( name=self.name , 
                                               **{'status':status,
                                               'data': date_utils.current_time()} )

    def run(self, config_db):

        self.set_status(status='INICIANDO', config_db=config_db)
        
        self.blacklist.add_black_list()

        self.set_status(status='TERMINADO', config_db=config_db)


class JobsEmr( object ):

    def __init__(self, upload, erm, name):
        setattr(self, 'upload_s3', upload)
        setattr(self, 'aws_erm', erm)
        setattr(self, 'name', name)

    def set_status(self, status, config_db):
        config_db.update_aws_script_mapper( name=self.name , 
                                            **{'status':status,
                                               'data': date_utils.current_time()} )
    def run(self, config_db):
        print 'executando jobs.. %s' % self.name

        self.set_status(status='UPLOAD_RAW_DATA', config_db=config_db)
               
        self.upload_s3.run()

        self.set_status(status='EXECUTANDO EMR', config_db=config_db)

        state = self.aws_erm.start_map_reduce()
        
        self.set_status(status=state.upper(), config_db=config_db)



class TimeService(Thread):

    def __init__(self, config_db):
        Thread.__init__(self)
        setattr(self, 'config_db', config_db)
        setattr(self, 'jobs', [])
        self.__create_jobs__()


    def __create_jobs__(self):

        name = 'FIND-STUDENTS'
        find_students = JobsEmr( upload=AwsS3AllTweetUpload(), 
                                 erm=EmrFilterStudents(name=name), name=name)

        name = 'WORD-COUNT'
        word_count    = JobsEmr( upload=AwsS3TweetsOfStudens(), 
                                 erm=EmrTweetCount(name=name),name=name )

        blacklist = JobsBlackList(name='BLACKLIST-TWITTER')

        self.jobs.append( find_students )
        self.jobs.append( word_count )
        self.jobs.append( blacklist )


    def __divisor_crontab__(self, num1, num2 ):
        retorno = []
        num1, num2 = int(num1),int(num2)
        while num1 >= 0:
           num1 = num1 - num2
           if num1 >= 0:
               retorno.append(str(num1))
        return retorno


    def run_crontab(self, compare, today):
        if compare == '*':
            return True
        if ',' in compare:
            return str(today) in compare.split(',')
        elif '/' in compare:
            (num1, num2) = compare.split('/')
            return str(today) in self.__divisor_crontab__(num1, num2)
        else:
            if compare[:1] == '0':
                compare = compare[:1]
            return str(today) == compare

        return False


    def check_contrab(self, **agendador):
        
        now = datetime.datetime.now()
        if not  self.run_crontab( agendador['diaSemana'], date.today().weekday()):
            return False
        if not self.run_crontab( agendador['minutos'], now.minute):
            return False
        if not self.run_crontab( agendador['horas'], now.hour):
            return False
        if not self.run_crontab( agendador['mes'], date.today().month):
            return False
        if not self.run_crontab( agendador['diaMes'], date.today().day):
            return False

        return True


    def run_jobs(self):
        for job in self.jobs:
            job.run( self.config_db )


    def run(self):

        while True:
            
            print 'rodando time_service..'

            for conf in list(self.config_db.get_config_emr()):
               if self.check_contrab( **conf['agendador'] ) is True:
                    self.run_jobs()
            return 0
            time.sleep( 32 )


    def execute(self, name):
        
        for job in self.jobs:
            if job.name == name.strip():
                print 'execuntando.. %s' % name
                job.run( self.config_db  )
          

