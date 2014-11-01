# -*- coding: utf-8 -*-

class TwitterJsonUtils(object):

    def remove_invalid_fields_from_json(self, tweet):
        if isinstance(tweet, dict):
            user_tweet = {}
            for key, values in tweet.iteritems():
                if isinstance(values, dict):
                   user_tweet[ str(key) ]  = self.remove_invalid_fields_from_json( values )
                elif isinstance(values, unicode):
                    user_tweet[ str(key) ] = values.encode('utf-8')
                elif isinstance(values, list):
                    tmp = []
                    for v in values:
                        if isinstance(v, dict):
                            tmp.append( self.remove_invalid_fields_from_json(v) ) 
                        else:
                            tmp.append( v )
                    user_tweet[ str(key) ] = tmp
                else:
                    user_tweet[ str(key) ] = values
            return user_tweet
        else:
            return {"TWEET" : str( type(tweet)), "VALOR " :  str( tweet ) }
