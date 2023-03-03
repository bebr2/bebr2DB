import eel
from eel import expose
from bebr2DB.Compiler import smm
from bebr2DB.settings import bcolors
from bebr2DB.Compiler import parse_web
@expose
def getAlltable():
    return smm.getAllDbandTableName()

@expose
def sql_in(sql):
    return parse_web(sql)

def close(a, b):
    try:
        smm.CloseDb()
    except:
        pass
    print(f"{bcolors.BOLD}Bye!{bcolors.ENDC}")
    exit(0)

print(f'{bcolors.OKGREEN}Welcome to BeBr2DB!{bcolors.ENDC}')
eel.init('./bebr2DB/Web')
eel.start('main.html', mode='chrome', port=8080, close_callback=close)
