#!/usr/bin/python
# -*- coding: utf-8 -*-
import web, sys
current_dir =  '/'.join( sys.path[0].split('/')[:-1])
sys.path.append(current_dir + '/python-libs/analyzers/')
from countstudents import *


urls = (
    '/api/v.1/countstudents/', 'CountStudents',
    '/', 'NotFoundError'
)

class CountStudents:

  def GET(self):



    web.header('Content-Type', 'application/json')
    return {'status':'ok'}



class NotFoundError(web.HTTPError):
    '''404 Not Found error.'''
    headers = {'Content-Type': 'application/json'}
    
    def __init__(self, note='Not Found', headers=None):
        status = '404 Not Found'
        message = json.dumps([{'note': note}])
        web.HTTPError.__init__(self, status, headers or self.headers,
                               unicode(message))

class Server(web.application):
    def run(self, port=5551, *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))

if __name__ == '__main__':
  app = Server(urls, globals())
  app.run()
