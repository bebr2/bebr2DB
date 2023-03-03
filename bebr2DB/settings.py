
PAGE_SIZE_BY_BYTE = 8192 #8KB
PAGE_SIZE_BY_BIT = PAGE_SIZE_BY_BYTE << 3
PAGE_SIZE_BY_INT_NUM = PAGE_SIZE_BY_BYTE >> 2 #32位整数个数
PAGE_SIZE_IDX = 13

MAX_FILE_NUM = 128

CAP = 60 #缓存页面数量上限
MOD = 60 #hash的mod

RECORD_FILE_HEAD_SIZE_BY_INT_NUM = 5
INDEX_FILE_HEAD_SIZE_BY_INT_NUM = 5

PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE = 4 #RM组件使用

class ATTR_TYPE():
    NONETYPE = 0
    INT = 1
    FLOAT = 2
    STRING = 3

class COMP_OP():
    EQ_OP = 0
    NE_OP = 1
    LT_OP = 2
    GT_OP = 3
    LE_OP = 4
    GE_OP = 5
    NO_OP = 6

class Aggregator():
    NO = 0
    COUNT = 1
    AVERAGE = 2
    MAX = 3
    MIN = 4
    SUM = 5

class WhereClause():
    EQ = 0
    NE = 1
    LT = 2
    GT = 3
    LE = 4
    GE = 5
    IS_NOT_NULL = 6
    IS_NULL = 7
    IN = 8
    LIKE = 9

#这个长度指的是转变成utf-8后的长度，中文会变长一点
MAX_STRING_LEN = 256
MAX_ATTR_COUNT = 40
MAX_NAME_LEN = 32

def STRING_EQUAL_FUNC(a:str, b:str):
    return a.rstrip() == b.rstrip()

OVERFLOWNODE_MAX_FILLING_NUMS = (PAGE_SIZE_BY_BYTE - 16) // 8

ATTRCAT_RECORD_SIZE = 324 + 128

class bcolors():
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'