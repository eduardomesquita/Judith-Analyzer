from pymongo import *

client = MongoClient()
client = MongoClient('localhost', 27017)




# Twitter db

db = client['judith-twitter']

db.create_collection('searchTags')
db.create_collection('twitters')

db['twitters'].ensure_index([('id_str',1), ("unique" , True)])
db['searchTags'].insert({"keysWords" : [ "unipam"], "last_tweet_text" : "", "language" : "pt"})
