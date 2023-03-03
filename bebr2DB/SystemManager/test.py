import os
from pathlib import Path


dr = os.path.join(Path(os.path.abspath(os.path.dirname(__file__))).parent, "DataBase")

print(os.listdir(dr))

# l = ['a', 'b', 'c']

# c = ['a', 'b']


# print(list(set(l)-set(c)))

class A:
    def __init__(self, string) -> None:
        self.string = string

    def check_db_open(f):
        def decorated(self, *args, **kwargs):
            print(self.string)
            return f(self, *args, **kwargs)
        return decorated

    @check_db_open
    def func(self, a, b):
        print(f'打印{a},{b}')
    
a = A("你好")

a.func(2, 3)