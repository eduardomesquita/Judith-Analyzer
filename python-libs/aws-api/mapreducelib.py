
from boto.emr.step import StreamingStep
from boto.emr.connection import EmrConnection
from boto.s3.connection import Location
import credentialsAWS as credentials_AWS
import boto.emr, time


class AwsMapReduce(object):

    def __init__(self,):
        setattr(self, 'AWSKEY', USER_AUTH['AWSAccessKeyId'])
        setattr(self, 'SECRETKEY', USER_AUTH['AWSSecretKey'])

    def __show_region_name__(self):
        for regions in boto.emr.regions():
            print regions

    def __connect_instance_emr__(self):
        kw_params = { 'aws_access_key_id': self.AWSKEY, 'aws_secret_access_key' : self.SECRETKEY}
        conn = boto.emr.connect_to_region('sa-east-1', **kw_params) 
        return conn

    def __show_process_log__(self, job_id, state, conn ):
          while state not in ['COMPLETED','FAILED']:
            print "job state = %s - job id = %s " %  ( state, job_id )
            time.sleep(30)
            state = conn.describe_jobflow(job_id).state
          
          return state

    def create(self, map_reduce_name, file_input, file_output, log_file, n_instance = 1):
        conn = self.__connect_instance_emr__()
        #mapper='s3n://mywordcounteduardo/mywordcount.py',
        step = StreamingStep(  name=map_reduce_name,
                               mapper='s3n://judith-project/scripts/v3/studentsfiltermapreduce.py',
                               reducer='aggregate',
                               input=file_input,
                               output=file_output )
        
        job_id = conn.run_jobflow( name=map_reduce_name + '-jobflow',
                                   log_uri=log_file,
                                   steps=[step],
                                   num_instances= n_instance )

        state = conn.describe_jobflow(job_id).state

        if state == 'STARTING':
            state  = self.__show_process_log__( job_id = job_id,
                                                state = state,
                                                conn = conn )

        print "job state = %s - job id = %s " %  ( state, job_id )



USER_AUTH = credentials_AWS.read_credential()