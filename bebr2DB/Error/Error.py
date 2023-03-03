from ..utils import bcolors

class myErr(Exception):
    def __init__(self, typ_, str_) -> None:
        super().__init__()
        self.str_ = str_
        self.typ_ = typ_
    
    def __str__(self) -> str:
        return f'{bcolors.FAIL}bebr2DB.Error.{self.typ_}: {self.str_}{bcolors.ENDC}'
