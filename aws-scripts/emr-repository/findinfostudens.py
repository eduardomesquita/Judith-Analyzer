#!/usr/bin/python
import re, sys



def main(argv): 
    
    for line in sys.stdin:
        infos = {}
        for info in line.split(';'):
            if len(info.split('=')) == 2:
                (key, value) = info.split('=')
                if key in ['user.screen_name','coordinates','full_name','geo','country_code','location','user.name','place','created_at','status_students']:
                   if value and value != 'None':
                       infos[key] = value
       
        if infos != {}:
            print "LongValueSum:ESTUDANTE;%s\t%s" % (str(infos), 1)


if __name__ == "__main__": 
    main(sys.argv) 
