import antlr4
from .SQLLexer import SQLLexer
from .SQLVisitor import SQLVisitor
from .SQLParser import SQLParser
import sys
from ..SystemManager import SM_Manager
from ..FileSystem import BufPageManager
from ..RecordManager import RM_Manager
from ..Index import IX_Manager
from ..settings import *
import re

bpm = BufPageManager()
ixm = IX_Manager(bpm)
rmm = RM_Manager(bpm)
smm = SM_Manager(ixm, rmm, bpm)

class redirect:
    content = ""
    def write(self,str):
        self.content += str
    def flush(self):
        self.content = ""


def parse(sql_string):
    if sql_string in ["q", "quit"]:
        smm.close()
        bpm.close()
        print(f"{bcolors.BOLD}Bye!{bcolors.ENDC}")
        exit(0)
    sSQL = antlr4.InputStream(sql_string)
    iLexer = SQLLexer(sSQL)
    sTokenStream = antlr4.CommonTokenStream(iLexer)
    iParser = SQLParser(sTokenStream)

    #词法错误：
    r = redirect()
    sys.stderr = r
    try:
        iTree = iParser.program()
    except Exception as e:
        print(e)
        return
    sys.stderr = sys.__stderr__
    if r.content != "":
        print(f'{bcolors.WARNING}Syntax Error{bcolors.ENDC}:',end="")
        if r.content.endswith('\n'):
            print(r.content, end='') 
        else:
            print(r.content) 
        return
    iVisitor = SQLVisitor(smm)
    iVisitor.visit(iTree)

def parse_web(sql_string):
    sSQL = antlr4.InputStream(sql_string)
    iLexer = SQLLexer(sSQL)
    sTokenStream = antlr4.CommonTokenStream(iLexer)
    iParser = SQLParser(sTokenStream)

    #词法错误：
    r = redirect()
    sys.stderr = r
    try:
        iTree = iParser.program()
    except Exception as e:
        print(e)
        return
    sys.stderr = sys.__stderr__
    if r.content != "":
        return [False, f"SyntaxError:{r.content.rstrip()}"]
    iVisitor = SQLVisitor(smm)
    return iVisitor.web_visit(iTree)

# parse("SELECT a FROM b;")