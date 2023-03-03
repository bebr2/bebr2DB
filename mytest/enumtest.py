class ATTR_TYPE():
    INT = 1
    FLOAT = 2
    STRING = 3


def j(attrtype):
    if attrtype == ATTR_TYPE.FLOAT:
        print("D")
    else:
        print("j")

j(2)