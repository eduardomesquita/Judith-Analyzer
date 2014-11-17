from studentsanalyzer import *
import thread

class AnalyzerProxy(object):

    def __init__(self):
        setattr(self,'students_analyzer', StudentsAnalyzer())
        self.start()

    def start(self):
        try:
            thread.start_new_thread( self.students_analyzer.init, () )
        except Exception as e:
            print "Erro: nao foi possivel iniciar thread. %s" % e
        
    def get_students_analyzer(self):
        return self.students_analyzer
        

if __name__ == '__main__':
    print AnalyzerProxy().get_students_analyzer()