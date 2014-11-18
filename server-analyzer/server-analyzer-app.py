#!/usr/bin/python
# -*- coding: utf-8 -*-
import web, sys, json, re
current_dir =  '/'.join( sys.path[0].split('/')[:-1])
sys.path.append(current_dir + '/python-libs/analyzers/')
sys.path.append(current_dir + '/python-libs/connectores/')
from proxyanalyzer import *
from twitterdb import *
from configdb import ConfigDB

urls = (
    '/api/v.1/students/porcentStatus', 'StudentsPorCent',
    '/api/v.1/students/location', 'StudentsLocation',
    '/api/v.1/mediassocais/getkeywords', 'GetKeyWords',
    '/api/v.1/mediassocais/excluirKeywords', 'ExcluirKeyWords',
    '/api/v.1/mediassocais/salvarKeywords', 'SalvarKeyWords',
    '/api/v.1/mapreduce/getmapreduces', 'GetMapReduce',
    '/api/v.1/estudantes/getstatusestudantes/(.*)', 'GetStudentsStatus',
    '/', 'NotFoundError'
)

def unquote(url):
  return re.compile('%([0-9a-fA-F]{2})',re.M).sub(lambda m: chr(int(m.group(1),16)), url)

global twitter_db
global config_db

config_db = ConfigDB()
twitter_db =  TwitterDB()


global proxy_analyzer 
proxy_analyzer = AnalyzerProxy()

class StudentsPorCent:
  def GET(self):
    global proxy_analyzer
    student = proxy_analyzer.get_students_analyzer()
    web.header('Content-Type', 'application/json')
    return json.dumps(student.status_users)

class StudentsLocation:
  def GET(self):
    global proxy_analyzer
    student = proxy_analyzer.get_students_analyzer()
    web.header('Content-Type', 'application/json')
    return json.dumps(student.location)


class GetKeyWords:
    def GET(self):
        global twitter_db
        web.header('Content-Type', 'application/json')
        return json.dumps(twitter_db.get_keys_word())

class ExcluirKeyWords:

    def decode_url(self, data):
        request = unquote(data).split('=')[1]
        keys_words = []
        for word in  request.split('#'):
            if word != '':
                 keys_words.append(word.strip().upper())
        return keys_words

    def POST(self):
        data =  web.data()
        web.header('Content-Type', 'application/json')

        try:
            global twitter_db
            keys_words = self.decode_url( data )
            twitter_db.remove_keyswords( keys_words )
            return json.dumps({'status': 'ok'})

        except:
            return json.dumps({'status': 'ERRO'})

class SalvarKeyWords:

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
        data =  web.data()
        web.header('Content-Type', 'application/json')

        try:
           global twitter_db
           keys_words = self.decode_url( data )
           twitter_db.save_key_words( **keys_words )
           return json.dumps({'status': 'ok'})
        except Exception as ex:
            print ex
            return json.dumps({'status': 'ERRO'})



class GetMapReduce:

    def GET(self):
        global config_db
        web.header('Content-Type', 'application/json')
        return json.dumps(list( config_db.get_jobs_emr() ))


class GetStudentsStatus:
    def GET(self, status):
        global proxy_analyzer
       
        web.header('Content-Type', 'application/json')
        return json.dumps( proxy_analyzer.get_students_count_tweet(status))




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

  app = Server(urls, globals())
  app.run()
