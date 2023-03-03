from ..bebr2DB.Error import myErr
import traceback

def f(a):
    if a == 1:
        raise myErr("no", "hhh")

try:
    f(1)
except Exception as e:
    print(e)