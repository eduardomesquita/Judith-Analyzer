import platform, sys

def read_credential():

    if  platform.node() == 'eduardo-linux':
        PATH = '/home/eduardo/Projetos/TCC/keys/aws_keys.csv'
    else:
        PATH ='/home/ubuntu/judith-project/keys/aws_keys.csv'
        
    return dict( [  line.strip().split('=') 
                            for line in open( PATH, 'r')] )