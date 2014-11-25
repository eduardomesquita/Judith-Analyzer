#!/usr/bin/python
import re, sys

def get_info(info):
    (key, value) = info.split('=')
    if key in ['user.location','text','status_students']:
        return key, value
    return None, None

def filter_data(line):
    keys = {'user.location':None, 'text': None, 'status_students': None}
    for info in line.split(';'):
        if len(info.split('=')) == 2:
            key, value = get_info(info)
            if key and value:
               keys[ key ] = value
    return keys


def word_count(data):
    pattern = re.compile("[a-zA-Z][a-zA-Z0-9]*")
    for word in  pattern.findall(str(data['text'])):
        print  "LongValueSum:%s;%s;%s\t1" % (data['status_students'], data['user.location'], word)

def main(argv):
    for line in sys.stdin:
        line = line.strip()

        data_fiter = filter_data(line)
        word_count( data_fiter )
                       
    
if __name__ == "__main__": 
    main(sys.argv) 
