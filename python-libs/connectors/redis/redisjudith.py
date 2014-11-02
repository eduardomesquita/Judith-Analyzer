import redis

class RedisJudith(object):

    def __init__(self, db = 0, host = 'localhost', port=6379):
        conexao = redis.StrictRedis(host= host, port= port, db= db)
        setattr(self, 'redis_db', conexao)

    def rpush(self, chave, value):
        return self.redis_db.rpush( key  , value )

    def left_pop(self, key):
        return self.redis_db.lpop( key )

    def llen(self, key):
        return self.redis_db.llen( key )

    def hset(self, name, key, value):
        return self.redis_db.hset(name, key, value)

    def hdel(self, name, chave):
        return self.redis_db.hdel(name, chave)

    def hgetall(self, name):
        return self.redis_db.hgetall(name)

    def hkeys(self, name):
        return self.redis_db.hkeys(name)

    def setex(self, name ,value, expire = 60 ):
        return self.redis_db.setex( name, expire, value )

    def get(self, name):
        return self.redis_db.get(name)

    def set(self, name, value):
        return self.redis_db.set(name, value)

    def clear(self):
        self.redis_db.flushall()

    def delete(self, name):
        return self.redis_db.delete(name)



if __name__ == '__main__':
    redis = RedisJudith()
    

    #redis.set(name='eduardoMea', value='aa' )
    print redis.get(name='luccoettes')
    #print redis.delete( name='luccoettes' )
    
