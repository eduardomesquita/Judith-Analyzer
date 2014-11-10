from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.key import Key
import credentialsAWS as credentials_AWS
import  sys, os


class S3Connector(object):
    def __init__(self):
        USER_AUTH = credentials_AWS.read_credential()
        setattr(self, 'conn', S3Connection( USER_AUTH['AWSAccessKeyId'], USER_AUTH['AWSSecretKey']))

    def __show_region_name__(self):
        print '\n'.join(i for i in dir(Location) if i[0].isupper())

    def create_bucket(self, bucket_name):
        try:
            return self.conn.create_bucket( bucket_name, location=Location.SAEast )
        except Exception as ex:
            raise Exception( ex )

    def get_bucket(self, bucket_name):
        return self.conn.get_bucket( bucket_name )

    def set_string(self, bucket_name, folder, key_name, text ):
        bucket = self.get_bucket( bucket_name )
        k = Key( bucket )
        k.key = folder +'/'+ key_name
        k.set_contents_from_string( text )

    def upload_file(self, bucket_name, file_name, path_file ):
        bucket = self.get_bucket( bucket_name )
        k = Key( bucket )
        k.key = file_name
        k.set_contents_from_filename(path_file)

    def list_content_of_bucket(self, bucket_name):
        bucket = self.get_bucket( bucket_name )
        bucket_list = bucket.list()
        contents = []
        for l in bucket_list:
            contents.append( l )

        return contents
            
        
           

        


