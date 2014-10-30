
from boto.emr.step import StreamingStep
from boto.emr.connection import EmrConnection
from boto.s3.connection import Location
import boto.emr, time


class AwsMapReduce(object):

    def __init__(self, aws_key, secret_key):  
        setattr(self, 'AWSKEY', aws_key)
        setattr(self, 'SECRETKEY', secret_key)

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
            time.sleep(5)
            state = conn.describe_jobflow(job_id).state
          
          return state

    def create(self, s3_bucket_name, n_instance):

        conn = self.__connect_instance_emr__()
  
        step = StreamingStep(  name='My wordcount example',
                               mapper='s3n://mywordcounteduardo/mywordcount.py',
                               reducer='aggregate',
                               input='s3n://mywordcounteduardo/teste.txt',
                               output='s3n://'+s3_bucket_name+'/output2/' )
        
        job_id = conn.run_jobflow( name='My jobflow',
                                   log_uri='s3://'+s3_bucket_name+'/jobflow_logs',
                                   steps=[step],
                                   num_instances= n_instance )

        state = conn.describe_jobflow(job_id).state

        if state == 'STARTING':
            state  = self.__show_process_log__( job_id = job_id,
                                                state = state,
                                                conn = conn )

        print "job state = %s - job id = %s " %  ( state, job_id )



map_reduce = AwsMapReduce( )
map_reduce.create(s3_bucket_name = 'mywordcounteduardo', n_instance = 1)
