#!/usr/bin/python
# -*- coding: utf-8 -*-
import web, sys, json, re, time
current_dir =  '/'.join( sys.path[0].split('/')[:-1])
sys.path.append(current_dir + '/python-libs/analyzers/')
sys.path.append(current_dir + '/python-libs/connectores/')
sys.path.append(current_dir + '/python-libs/utils/')

from proxyanalyzer import *
from twitterdb import *
from configdb import ConfigDB
from collections import OrderedDict
import dateutils as date_utils
from timeservice import TimeService

urls = (
    
    '/api/v.1/graphs/estudantes/porcentStatus', 'GraphicsStatusPorCent',
    '/api/v.1/graphs/estudantes/location', 'GraphicsStudentsLocation',

    '/api/v.1/mediassocais/get/tweet/keywords', 'GetAllKeyWord',
    '/api/v.1/mediassocais/delete/tweet/Keywords', 'DeleteKeyWord',
    '/api/v.1/mediassocais/save/tweet/keywords', 'SaveKeyword',
    
    '/api/v.1/mapreduce/get/mapreduces', 'GetJobsMapReduce',

    '/api/v.1/estudantes/get/status/(.*)', 'GetStatusStudents',
    '/api/v.1/estudantes/get/tweet/usersname/(.*)', 'GetTweetsByUserName',
    '/api/v.1/estudantes/blacklist/usersname/', 'AddBlackListUserName',
    '/api/v.1/estudantes/get/blacklist/(.*)', 'GetBlacklist',
    '/api/v.1/estudantes/remove/blacklist/', 'RemoveBlacklist',

    '/api/v.1/configuracoes/update/', 'UpdateConfig',
    '/api/v.1/configuracoes/get/', 'GetConfig',
    '/api/v.1/configuracoes/executar/', 'ExecutarEmr',
    
    '/', 'NotFoundError'
)



global twitter_db
global config_db
global proxy_analyzer
global time_service

config_db = ConfigDB()
twitter_db =  TwitterDB()
proxy_analyzer = AnalyzerProxy()
time_service = TimeService( config_db )


def unquote(url):
  return re.compile('%([0-9a-fA-F]{2})',re.M).sub(lambda m: chr(int(m.group(1),16)), url)

## Graficos 

class GraphicsStatusPorCent:
  def GET(self):
    global proxy_analyzer
    #student = proxy_analyzer.get_students_analyzer()
    #web.header('Content-Type', 'application/json')
    #return json.dumps(student.status_users)

class GraphicsStudentsLocation:
  def GET(self):
    global proxy_analyzer
    #student = proxy_analyzer.get_students_analyzer()
    #web.header('Content-Type', 'application/json')
    #return json.dumps(student.location)



## Tweets

class GetAllKeyWord:
    def GET(self):
        global twitter_db
        web.header('Content-Type', 'application/json')
        return json.dumps(twitter_db.get_keys_word())

class DeleteKeyWord:

    def decode_url(self, data):
        request = unquote(data).split('=')[1]
        keys_words = []
        for word in  request.split('#'):
            if word != '':
                 keys_words.append(word.strip().upper())
        return keys_words

    def POST(self):
        global twitter_db
        data =  web.data()
        web.header('Content-Type', 'application/json')

        try:
            keys_words = self.decode_url( data )
            twitter_db.remove_keyswords( keys_words )
            response = json.dumps({'status': 'ok'})
        except:
            response = json.dumps({'status': 'ERRO'})

        web.header('Content-Type', 'application/json')
        return response

class SaveKeyword:

    def decode_url(self, data):
        retorno = {}
        params = unquote(data).split('&')
        for values in params:
            (key, valor) = values.split('=')
            if key == 'keysWords':
                if valor != '':
                    retorno[key] = [item.upper() for item in valor.split('#') if item != '']
                else:
                    raise Exception('values vazio')
            else:
                retorno[key] = valor
        return retorno

    def POST(self):
        global twitter_db
        data =  web.data()
        try:
           
           keys_words = self.decode_url( data )
           twitter_db.save_key_words( **keys_words )
           response =  json.dumps({'status': 'ok'})
        except Exception as ex:
           response = json.dumps({'status': 'ERRO', 'ERRO': str(ex)})

        web.header('Content-Type', 'application/json')
        return response

class GetJobsMapReduce:
    def GET(self):
        global config_db
        web.header('Content-Type', 'application/json')
        return json.dumps(list( config_db.get_jobs_emr() ))

class GetStatusStudents:
    def GET(self, status):
        global proxy_analyzer
        global twitter_db

        blacklist = []
        for bjson in  list(twitter_db.find_all_data_black_list()):
            blacklist.append(bjson['username'])

        retorno = []
        for students in proxy_analyzer.get_students_count_tweet(status):
            if students['userName'] not in blacklist:
                retorno.append( students  )

        web.header('Content-Type', 'application/json')
        return json.dumps( retorno  )

class GetTweetsByUserName:

    def order_by(self, cursor):
        created_at = {}
        for bjson in  list(cursor):
            ts = time.strftime('%Y-%m-%d %H:%M:%S',
                        time.strptime(bjson['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            created_at[ts] = {'text': bjson['text'], 'name' :bjson['user']['name'], 'created_at' : ts }

        retorno = []
        for key in OrderedDict(sorted(created_at.items(), key=lambda t: t[0])):
            retorno.append( created_at[key] )
        return retorno

    def GET(self, user):
        global twitter_db
        cursor = twitter_db.find_raw_data_users(user)
        web.header('Content-Type', 'application/json')
        return json.dumps(self.order_by( cursor ))


class AddBlackListUserName:

    def decode_url(self, data):
        retorno = {}
        for p in unquote(data).split('&'):
            params = p.split('=')
            if len(params) == 2:
                (key, value) = params
                retorno[key] = value
            else:
                raise Exception('parametros invalidos')
        return retorno


    def POST(self):
        global twitter_db
        try:
            data =  web.data()
            username = self.decode_url( data )
            username['created_at'] = date_utils.current_time()
            twitter_db.insert_user_name_in_black_list( **username)
            web.header('Content-Type', 'application/json')
            return json.dumps({'status':'ok'})
        except Exception as ex:
            return json.dumps({'status':'erro', 'ERRO': str(ex)})


class GetBlacklist:

    def GET(self, params):
        global twitter_db
        blacklist = []
        for bjson in  list(twitter_db.find_all_data_black_list()):
            blacklist.append({'username' : bjson['username'], 'created_at' : bjson['created_at']})

        return json.dumps(blacklist)

class RemoveBlacklist:

    def decode_url(self, data):
        retorno = {}
        for p in unquote(data).split('&'):
            params = p.split('=')
            if len(params) == 2:
                (key, value) = params
                retorno[key] = value
            else:
                raise Exception('parametros invalidos')
        return retorno


    def POST(self):
        global twitter_db
        try:

            data =  web.data()
            username = self.decode_url( data )
            twitter_db.remove_black_list( **username )
            web.header('Content-Type', 'application/json')
            return json.dumps({'status':'ok'})

        except Exception as ex:
            return json.dumps({'status':'erro', 'ERRO': str(ex)})


## configuracoes 


class UpdateConfig:

    def decode_url(self, data):
        retorno = {}
        for p in unquote(data).split('&'):
            params = p.split('=')
            if len(params) == 2:
                (key, value) = params
                retorno[key] = value
            else:
                raise Exception('parametros invalidos')
        return retorno

    def get_agendador(self, **config ):
        agendador =  config['agendador'].split(' ')
        if len(agendador) != 5:
            raise Exception('erro agendandor incorreto')
        (minutos, horas, diaMes, mes, diaSemana) = agendador
        config['agendador'] = { 'minutos': minutos,
                                'horas': horas,
                                'diaMes': diaMes,
                                'mes': mes,
                                'diaSemana': diaSemana }
        return config

    def POST(self):

        try:
            data =  web.data()
            config = self.decode_url( data )
            global config_db
            config = self.get_agendador( **config )
            config_db.update_config_emr( **config )
            
            response =  json.dumps({'status': 'ok'})
        except Exception as ex:
           response = json.dumps({'status': 'ERRO', 'ERRO': str(ex)})

        web.header('Content-Type', 'application/json')
        return response

class GetConfig:

    def GET(self):
        global config_db
        config =  list(config_db.get_config_emr())

        config[0]['scripts'] = []
        for bjson in list(config_db.get_all_path_aws_script_mapper()):
            bjson['type'] = 'MAPREDUCE'
            config[0]['scripts'].append( bjson )

        for bjson in list(config_db.get_all_path_aws_script_blacklist()):
            bjson['type'] = 'BLACKLIST'
            config[0]['scripts'].append( bjson )


        web.header('Content-Type', 'application/json')
        del config[0]['_id']
        return json.dumps( config )


class ExecutarEmr:

    def decode_url(self, data):
        retorno = {}
        for p in unquote(data).split('&'):
            params = p.split('=')
            if len(params) == 2:
                (key, value) = params
                retorno[key] = value
            else:
                raise Exception('parametros invalidos')
        return retorno

    def POST(self):

        #try:
            data =  web.data()
            emr = self.decode_url( data )
            time_service.execute( emr['name'] )

        #except Exception as ex:
        #   response = json.dumps({'status': 'ERRO', 'ERRO': str(ex)})




## SERVER CONFIG

class NotFoundError(web.HTTPError):
    '''404 Not Found error.'''
    headers = {'Content-Type': 'application/json'}
    
    def __init__(self, note='Not Found', headers=None):
        status = '404 Not Found'
        message = json.dumps([{'note': note}])
        web.HTTPError.__init__(self, status, headers or self.headers,
                               unicode(message))
        
class Server(web.application):
    def run(self, port=5222, *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))



if __name__ == '__main__':
  
  
  #time_service.start()

  app = Server(urls, globals())
  app.run()