
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

    def create(self, name, input_file, output_file, log_file, mapper, n_instance = 1):
        conn = self.__connect_instance_emr__()
        step = StreamingStep(  name=name,
                               mapper=mapper,
                               reducer='aggregate',
                               input=input_file,
                               output=output_file )
        
        job_id = conn.run_jobflow( name=name + '-jobflow',
                                   log_uri=log_file,
                                   steps=[step],
                                   num_instances= n_instance )

        state = conn.describe_jobflow(job_id).state

        if state == 'STARTING':
            state  = self.__show_process_log__( job_id = job_id,
                                                state = state,
                                                conn = conn )

        print "job state = %s - job id = %s " %  ( state, job_id )
        return state, job_id

        
USER_AUTH = credentials_AWS.read_credential()

if __name__ == '__main__':
  AwsMapReduce().create('teste', 's3n://judith-project/raw_data/2014-11-09-20-36-20/raw_data_twitter', 's3n://judith-project/saida/', 's3n://judith-project/logs/')