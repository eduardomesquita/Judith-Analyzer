from pymongo import *

client = MongoClient()
client = MongoClient('localhost', 27017)

# config db

db = client['judith-project-config']
db['parameters'].insert({"key": "path_aws_uploads", "value": "/var/uploads_aws_files"})


# Twitter db

db = client['judith-twitter']

db.create_collection('searchTags')
db.create_collection('searchUsers')
db.create_collection('twittersTags')
db.create_collection('twittersUsers')


db['twittersTags'].ensure_index([('id_str',1), ('unique' , True)])
db['twittersUsers'].ensure_index([('id_str',1), ('unique' , True)])
db['searchUsers'].ensure_index([('user_name',1), ('unique' , True)])
db['searchTags'].ensure_index([('user_name',1), ('unique' , True)])

#db['searchTags'].insert({'keysWords' : [ 'unipam'], 'last_tweet_text' : '', 'language' : 'pt'})
