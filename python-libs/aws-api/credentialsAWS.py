import platform, sys

def read_credential():

    if  platform.node() == 'eduardo-linux':
        PATH = '/home/eduardo/Projetos/TCC/keys/aws_keys.csv'
    else:
        project_path = '/'.join( sys.path[0].split('/')[:-4] )
        PATH = project_path + '../../../keys/aws_keys.csv'
    return dict( [  line.strip().split('=') 
                            for line in open( PATH, 'r')] )