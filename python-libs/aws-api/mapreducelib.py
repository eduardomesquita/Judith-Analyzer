from boto.emr.step import StreamingStep
from boto.emr.connection import EmrConnection
from boto.s3.connection import Location
import credentialsAWS as credentials_AWS
import boto.emr, time


class AwsMapReduce(object):

    def __init__(self, map_reduce_config):
      setattr(self, 'AWSKEY', USER_AUTH['AWSAccessKeyId'])
      setattr(self, 'SECRETKEY', USER_AUTH['AWSSecretKey'])

      for conf in map_reduce_config:
        for key, values in conf.iteritems():
           setattr(self, key, values)

    def get_instance_number(self):
      return int(self.nInstancia)

    def get_instance_master(self):
      return self.masterInstance

    def get_instance_slave(self):
      return self.slaveInstance

    def get_region(self):
      return self.localizacao

    def __connect_instance_emr__(self):
      kw_params = { 'aws_access_key_id': self.AWSKEY, 'aws_secret_access_key' : self.SECRETKEY}
      conn = boto.emr.connect_to_region(self.get_region(), **kw_params) 
      return conn

    def __log_emr__(self, job_id, state, conn ):
      while state not in ['COMPLETED','FAILED']:
        print "job state = %s - job id = %s " %  ( state, job_id )
        time.sleep(30)
        state = conn.describe_jobflow(job_id).state
      return state

    def create(self, name, input_file, output_file, log_file, mapper):
        print 'criando mapreduce com %s instancia' % self.get_instance_number()
        print 'criando mapreduce com master %s slave %s' % (self.get_instance_master(), self.get_instance_slave())

        conn = self.__connect_instance_emr__()
        step = StreamingStep(  name=name,
                               mapper=mapper,
                               reducer='aggregate',
                               input=input_file,
                               output=output_file )
        
        job_id = conn.run_jobflow( name=name + '-jobflow',
                                   log_uri=log_file,
                                   steps=[step],
                                   master_instance_type=self.get_instance_master(),
                                   slave_instance_type=self.get_instance_slave(),
                                   num_instances=self.get_instance_number())

        state = conn.describe_jobflow(job_id).state

        if state == 'STARTING':
            state  = self.__log_emr__( job_id = job_id, state = state, conn = conn )
        print "job state = %s - job id = %s " %  ( state, job_id )
        return state, job_id

USER_AUTH = credentials_AWS.read_credential()