from pymongo import *

client = MongoClient()
client = MongoClient('localhost', 27017)

# config db

db = client['judith-project-config']
#db['parameters'].insert({"key": "path_aws_uploads", "value": "/var/uploads_aws_files"})


# Twitter db

db = client['judith-twitter']

#db.create_collection('searchTags')
#db.create_collection('searchUsers')
#db.create_collection('twittersTags')
#db.create_collection('twittersUsers')
#db.create_collection('blacklist')

#db.blacklist.ensureIndex( { "username": 1 }, { unique: true, dropDups: true } )

#db.collection.ensureIndex( { a: 1 }, { unique: true, dropDups: true } )


db.emrJobsConfig.insert({"name":"find-students", "count":0, "n_instance":1, "region":"sa-east-1", "master_instance":"m1.small", "slaver_instance":"m1.small" })
db.emrJobsConfig.insert({"name":"word-count", "count":0, "n_instance":1, "region":"sa-east-1", "master_instance":"m1.small", "slaver_instance":"m1.small" })


db.studentsCountTweet.ensureIndex( { "userName": 1 }, { unique: true } )
db.twittersUsers.ensureIndex( { "user.screen_name": 1}, {background: true} )
db.studentsStatus.ensureIndex( { "userName": 1, "statusStudents" :1 }, { unique: true } )
db.cacheAnalyzer.ensureIndex( { "name" :1 }, { unique: true } )
db.wordCountStudents.ensureIndex( { "word": 1, "statusStudents" :1, "create_at": 1, "location" : 1 }, { unique: true } )

#db['twittersTags'].ensure_index([('id_str',1), ('unique' , True)])
#db['twittersUsers'].ensure_index([('id_str',1), ('unique' , True)])
#db['searchUsers'].ensure_index([('keysWords',1), ('unique' , True)])

#db['searchTags'].insert({'keysWords' : [ 'unipam'], 'last_tweet_text' : '', 'language' : 'pt'})
