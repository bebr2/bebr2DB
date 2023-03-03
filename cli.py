import click
from bebr2DB.Compiler import parse, smm
from bebr2DB.settings import bcolors
import colorama

class prompt_info():
    def getstr(self):
        return f'{bcolors.FAIL}({smm.dbName}){bcolors.ENDC}' if smm.open else ""
    
    def __add__(self, other):
        return f'{self.getstr()}>>{other}'

my_prompt_info = prompt_info()

@click.command()
@click.option('--message', prompt=my_prompt_info)
def hello(message):
    parse(message)
    hello()

if __name__ == "__main__":
    colorama.init(autoreset=True)
    print(f'{bcolors.OKGREEN}Welcome to BeBr2DB! Please type in "q" or "quit" instead of press down "Ctrl+C" to exit safely.{bcolors.ENDC}')
    hello()