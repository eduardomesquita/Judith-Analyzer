#!/usr/bin/python
# -*- coding: utf-8 -*-
import web, sys, json, re, time
from collections import OrderedDict

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
    '/api/v.1/graphs/estudantes/createdAtMoth', 'GraphicsStudentsCreatedAtMouth',
    '/api/v.1/graphs/estudantes/createdAtHour', 'GraphicsStudentsCreatedAtHour',
    '/api/v.1/graphs/estudantes/course', 'GraphicsStudentsCourse',

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

    '/api/v.1/logs/find/', 'FindLogs',
    
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
    student = proxy_analyzer.get_analysis(key='user_status_count')
    web.header('Content-Type', 'application/json')
    return json.dumps(student)

class GraphicsStudentsLocation:

    def clear_location(self, location):
        if '/' in location:
           location =  location.split('/')[0]
        if ',' in location:
           location =  location.split(',')[0]
        if '-' in location:
           location =  location.split('-')[0]

        return location.strip().split(' ')[0].upper()

    def clear(self, student):
        tmp = {}
        retorno = []
        for json in student:
           location=json['values']
           for i in location :
               for k, v in location[i]['location'].iteritems():
                   k = self.clear_location(k)
                   if not tmp.has_key(k):
                       tmp[k] = 0
                   tmp[k] += int(v)

        sorted_list  = OrderedDict(sorted(tmp.items(), key=lambda t: t[1]))
        for i in sorted_list:
            retorno.append({ i : sorted_list[i]})

        return retorno[::-1]
        
    
    def GET(self):
        global proxy_analyzer
        student = proxy_analyzer.get_analysis(key='user_status_location')
        retorno = self.clear( student )
        web.header('Content-Type', 'application/json')
        return json.dumps(retorno)



class GraphicsStudentsCreatedAtMouth:

    def GET(self):
        global proxy_analyzer
        data_raw = proxy_analyzer.get_analysis(key='user_status_created_at')
        moths = {}
        moths['student'] = {'1' : 0, '2' : 0 ,'3' : 0,'4' : 0, '5' : 0 ,'6' : 0, '7' : 0, '8' : 0 ,'9' : 0,'10' : 0, '11' : 0 ,'12' : 0}
        moths['possible'] = {'1' : 0, '2' : 0 ,'3' : 0,'4' : 0, '5' : 0 ,'6' : 0, '7' : 0, '8' : 0 ,'9' : 0,'10' : 0, '11' : 0 ,'12' : 0}

        for i in data_raw[0]:

            for j in data_raw[0][i]:
                result = data_raw[0][i][j]
                status =  result['status_users'] 
                mes = result['created_tweet_at']['month']
                for k , v in mes.iteritems():
                    moths[status][k] += int(v)
    
        return json.dumps(moths)



class GraphicsStudentsCreatedAtHour:

    def GET(self):
        global proxy_analyzer
        data_raw = proxy_analyzer.get_analysis(key='user_status_created_at')
        hours = {}
        hours['student'] = {'00' : 0, '01' : 0 ,'02' : 0,'03' : 0, '04' : 0 ,'05' : 0, '06' : 0, '07' : 0,
                            '08' : 0, '09' : 0, '10' : 0 ,'11' : 0,'12' : 0, '13' : 0, '14' : 0 ,'15' : 0,
                            '16' : 0, '17' : 0, '18' : 0 ,'19' : 0,'20' : 0,'21' : 0,'22' : 0,'23' : 0}
        hours['possible'] = {'00' : 0, '01' : 0 ,'02' : 0,'03' : 0, '04' : 0 ,'05' : 0, '06' : 0, '07' : 0,
                            '08' : 0, '09' : 0, '10' : 0 ,'11' : 0,'12' : 0, '13' : 0, '14' : 0 ,'15' : 0,
                            '16' : 0, '17' : 0, '18' : 0 ,'19' : 0,'20' : 0,'21' : 0,'22' : 0,'23' : 0}

        for i in data_raw[0]:

            for j in data_raw[0][i]:
                result = data_raw[0][i][j]
                status =  result['status_users'] 
                hour = result['created_tweet_at']['hour']
              
                for k , v in hour.iteritems():
                    hours[status][k] += int(v)
    
        return json.dumps(hours)


class GraphicsStudentsCourse:


    def __get_courses__(self, data_raw):
        courses = []
        for i in data_raw['values']['ESTUDANTE']:
            courses.append( i )
        
        for i in data_raw['values']['POSSIVEL']:
            courses.append( i )
        courses.sort()
        return courses


    def GET(self):
        global proxy_analyzer
        data_raw = proxy_analyzer.get_analysis(key='word_course_word_status')
        retorno = {}
        for i in self.__get_courses__( data_raw[0] ):
            retorno[i] =  {'POSSIVEL' : {}, 'ESTUDANTE': {}}

        for i in data_raw[0]['values']['ESTUDANTE']:
            retorno[i]['ESTUDANTE'] = data_raw[0]['values']['ESTUDANTE'][i]
        
        for i in data_raw[0]['values']['POSSIVEL']:
            retorno[i]['POSSIVEL'] = data_raw[0]['values']['POSSIVEL'][i]

        return json.dumps(retorno)


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
            print 'estou apagando a palavra... %s' % ( keys_words )
            twitter_db.remove_keyswords( keys_words )
            response = json.dumps({'status': 'ok'})
        except Exception as e:
            print 'erro  %s' % e
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
                    retorno[key] = [item.upper().strip() for item in valor.split('#') if item != '']
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
        return json.dumps(list( config_db.get_jobs_emr())[::-1])

class GetStatusStudents:

    def order_by(self, count_tweet, students):
        retorno = []
        
        for i in OrderedDict(sorted(count_tweet.items(), key=lambda t: t[0])):
            for j  in students:
               if j['totalTweet'] == i:
                   retorno.append(j) 

        return retorno[::-1]


    def GET(self, status):
        global proxy_analyzer
        global twitter_db

        blacklist = []
        for bjson in  list(twitter_db.find_all_data_black_list()):
            blacklist.append(bjson['username'])

        retorno = []
        count_tweet = {}
        for students in proxy_analyzer.get_students_count_tweet(status):
         
            if students['userName'] not in blacklist:
                count_tweet[students['totalTweet']] = 1
                retorno.append( students )

        web.header('Content-Type', 'application/json')
        return json.dumps( self.order_by(  count_tweet, retorno)  )

class GetTweetsByUserName:

    def order_by(self, cursors):

        created_at = {}
        for cursor in cursors:
            for bjson in  list(cursor):
                ts = time.strftime('%d-%m-%Y %H:%M:%S',
                            time.strptime(bjson['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
                
                created_at[ts] = {'text': bjson['text'], 'name' :bjson['user']['name'], 'created_at' : ts }

        retorno = []
        for key in OrderedDict(sorted(created_at.items(), key=lambda t: t[0])):
            retorno.append( created_at[key] )
        return retorno

    def GET(self, user):
        global twitter_db
        cursors = []
        cursors.append(  twitter_db.find_raw_data_users(user) ) 
        cursors.append(  twitter_db.find_raw_data_tags(user) )

        web.header('Content-Type', 'application/json')
        return json.dumps(self.order_by( cursors ))


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
            username['created_at'] = date_utils.current_time(fmt='%d-%m-%Y %H:%M:%S')
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

        try:
            data =  web.data()
            emr = self.decode_url( data )
            time_service.execute( emr['name'] )
            response = {'status' : 'ok'}
        except Exception as ex:
           response = json.dumps({'status': 'ERRO', 'ERRO': str(ex)})

        web.header('Content-Type', 'application/json')
        return json.dumps( response )



class FindLogs:

    def GET(self):
        global config_db
        web.header('Content-Type', 'application/json')
        
        retorno = []
        for item in list( config_db.find_log() ):
            del item['_id']
            retorno.append(item)

        response = json.dumps({ 'retorno': retorno })
        web.header('Content-Type', 'application/json')
        return response

   



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
  
  time_service.start()

  app = Server(urls, globals())
  app.run()