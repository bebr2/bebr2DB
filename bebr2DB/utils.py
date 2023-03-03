#一些通用的工具

from .settings import *
import numpy as np
import struct
# from .Error import myErr

def U_check_attr_type_and_length(attrType, attrLength):
    # sourcery skip: assign-if-exp, boolean-if-exp-identity, reintroduce-else, remove-unnecessary-cast
    if attrType < ATTR_TYPE.INT or attrType > ATTR_TYPE.STRING:
        return False

    if attrType == ATTR_TYPE.INT and attrLength != 4:
        return False

    if attrType == ATTR_TYPE.FLOAT and attrLength != 4:
        return False

    if attrType == ATTR_TYPE.STRING and (attrLength <= 0 or attrLength > MAX_STRING_LEN):
        return False

    return True

def U_int2byte(l: list, file_head_size = RECORD_FILE_HEAD_SIZE_BY_INT_NUM):
    '''
    返回的是整个页面
    '''
    def tobyte(c):
        return int(c).to_bytes(4, byteorder='big') #必须保证c是正整数
    e = [tobyte(c) for c in l]
    cc =  np.frombuffer(np.array(e), dtype=np.uint8)
    return np.pad(cc, (0, PAGE_SIZE_BY_BYTE - file_head_size * 4), 'constant', constant_values=(0, 0))

def U_unpack_attr(data, attr_type, attr_length = 4):
    rv = None
    if attr_type == ATTR_TYPE.INT:
        rv = struct.unpack('i', data)[0]
    elif attr_type == ATTR_TYPE.FLOAT:
        rv = struct.unpack('f', data)[0]
    elif attr_type == ATTR_TYPE.STRING:
        rv = data.tobytes().decode('utf-8').rstrip()
    return rv

def U_unpack_attr_array(data, attr_type, attr_length = 4, offset = 0, nums = 1):
    #返回一个np.ndarray，也就是array版本的U_unpack_attr
    rv = None
    if attr_type == ATTR_TYPE.INT:
        rv = np.array([struct.unpack('i', data[offset + 4 * i : offset + 4 * i + 4])[0] for i in range(nums)], dtype=int)
    elif attr_type == ATTR_TYPE.FLOAT:
        rv = np.array([struct.unpack('f', data[offset + 4 * i : offset + 4 * i + 4])[0] for i in range(nums)], dtype=np.float32)
    elif attr_type == ATTR_TYPE.STRING:
        rv = np.array([data[offset + attr_length * i : offset + attr_length * i + attr_length].tobytes().decode('utf-8') for i in range(nums)], dtype=f'<U{attr_length}')
        #注意是没有rstrip的！！！
    return rv

def U_pack_attr(raw_data, attr_type, attr_length = 4):
    #返回一个np.ndarray
    arr = None
    if attr_type == ATTR_TYPE.INT:
        data = struct.pack('i', int(raw_data))
        arr = np.frombuffer(data, dtype=np.uint8)
    elif attr_type == ATTR_TYPE.FLOAT:
        data = struct.pack('f', float(raw_data))
        arr = np.frombuffer(data, dtype=np.uint8)
    elif attr_type == ATTR_TYPE.STRING:
        data = raw_data.encode('utf-8')
        if len(data) > attr_length:
            raise Exception("The string is too long.")
        ini_arr = np.frombuffer(data, dtype=np.uint8)
        arr = np.pad(ini_arr, (0, attr_length - len(data)), 'constant', constant_values=(0, 32))
    return arr

def U_pack_attr_array(raw_data, attr_type, attr_length = 4, need_pad_to = 1):
    #返回一个np.ndarray
    arr = None
    if attr_type == ATTR_TYPE.INT:
        data = struct.pack(f'{len(raw_data)}i', *raw_data)
        arr = np.frombuffer(data, dtype=np.uint8)
    elif attr_type == ATTR_TYPE.FLOAT:
        data = struct.pack(f'{len(raw_data)}f', *raw_data)
        arr = np.frombuffer(data, dtype=np.uint8)
    elif attr_type == ATTR_TYPE.STRING:
        #这里就不进行长度检查了，并且默认给的字符串长度已经合适
        data = [U_pad_a_string(_data, attr_length).encode('utf-8') for _data in raw_data]
        arr = np.frombuffer(b''.join(data), dtype=np.uint8)
    arr = np.pad(arr, (0, need_pad_to * attr_length - len(arr)), 'constant', constant_values=(0, 0))
    return arr

def U_convertTo_uint32(a):
    '''
    注意页头和文件头的数据，都是用这种大端、uint32的格式储存
    不同于属性记录！
    a必须是长度为4的uint8的数组
    大端的转换
    '''
    la = 0x00FFFFFF | (np.uint32(a[0]) << 24)
    lb = 0xFF00FFFF | (np.uint32(a[1]) << 16)
    lc = 0xFFFF00FF | (np.uint32(a[2]) << 8)
    ld = 0XFFFFFF00 | np.uint32(a[3])
    return np.uint32(la & lb & lc & ld)


def U_pack_attrcat_data(dataAttrInfo):
    relName_b = dataAttrInfo["relName"].encode('utf-8')
    attrName_b = dataAttrInfo["attrName"].encode('utf-8')
    ft_b = dataAttrInfo["foreign_table"].encode('utf-8')
    fn_b = dataAttrInfo["foreign_key_name"].encode('utf-8')
    ini_arr1 = np.frombuffer(relName_b, dtype=np.uint8)
    pack_relName_b = np.pad(ini_arr1, (0, MAX_NAME_LEN - len(ini_arr1)), 'constant', constant_values=(0, 32))
    ini_arr2 = np.frombuffer(attrName_b, dtype=np.uint8)
    pack_attrName_b = np.pad(ini_arr2, (0, MAX_NAME_LEN - len(ini_arr2)), 'constant', constant_values=(0, 32))
    ini_arr3 = np.frombuffer(ft_b, dtype=np.uint8)
    pack_ft_b = np.pad(ini_arr3, (0, MAX_NAME_LEN - len(ini_arr3)), 'constant', constant_values=(0, 32))
    ini_arr4 = np.frombuffer(fn_b, dtype=np.uint8)
    pack_fn_b = np.pad(ini_arr4, (0, 2*MAX_NAME_LEN - len(ini_arr4)), 'constant', constant_values=(0, 32))

    pack_offest = U_pack_attr(dataAttrInfo["offset"], ATTR_TYPE.INT)
    pack_attrType = U_pack_attr(dataAttrInfo["attrType"], ATTR_TYPE.INT)
    pack_attrLength = U_pack_attr(dataAttrInfo["attrLength"], ATTR_TYPE.INT)
    pack_indexNo = U_pack_attr(dataAttrInfo["indexNo"], ATTR_TYPE.INT)
    pack_null = U_pack_attr(dataAttrInfo["null"], ATTR_TYPE.INT)
    pack_default_is_null = U_pack_attr(dataAttrInfo["default_is_null"], ATTR_TYPE.INT)
    pack_unique = U_pack_attr(dataAttrInfo["unique"], ATTR_TYPE.INT)
    pack_have_fk = U_pack_attr(dataAttrInfo["have_fk"], ATTR_TYPE.INT)
    pack_rank = U_pack_attr(dataAttrInfo["rank"], ATTR_TYPE.INT)
    
    if dataAttrInfo["default_is_null"] == 1:
        pack_default = U_pack_attr_array([0]*(MAX_STRING_LEN//4), ATTR_TYPE.INT, 4, (MAX_STRING_LEN//4))
    elif dataAttrInfo["attrType"] == ATTR_TYPE.INT:
        l = [0]*(MAX_STRING_LEN//4)
        l[0] = dataAttrInfo["default"]
        pack_default = U_pack_attr_array(l, ATTR_TYPE.INT, 4, (MAX_STRING_LEN//4))
    elif dataAttrInfo["attrType"] == ATTR_TYPE.FLOAT:
        l = [0]*(MAX_STRING_LEN//4)
        l[0] = dataAttrInfo["default"]
        pack_default = U_pack_attr_array(l, ATTR_TYPE.FLOAT, 4, (MAX_STRING_LEN//4))
    else:
        ini_arr5 = np.frombuffer(dataAttrInfo["default"], dtype=np.uint8)
        pack_default = np.pad(ini_arr5, (0, MAX_STRING_LEN - len(ini_arr5)), 'constant', constant_values=(0, 32))
    # null, default, default_is_null, unique, have_fk, foreign_table, foreign_key_name
    #32 + 32 + 4 + 4 + 4 + 4 + 4 + 128 + 4 + 4 + 4 + 32 + 32 + 4
    return np.concatenate((pack_relName_b, pack_attrName_b, pack_offest, pack_attrType, pack_attrLength, pack_indexNo, pack_null, pack_default, pack_default_is_null,
    pack_unique, pack_have_fk, pack_ft_b, pack_fn_b, pack_rank))




def U_pad_a_string(string, length):
    b_string = string.encode('utf-8')
    if len(b_string) >= length:
        return string
    need = length - len(b_string)
    l = [32] * need
    pad_b_string = b_string + struct.pack(f'{need}b', *l)
    return pad_b_string.decode('utf-8')

def U_check_if_attr_is_null(data, rank):
    index = rank // 8
    offset = rank % 8
    return (data[index] >> (7 - offset)) & 1

def U_change_LIKESTRING_to_re(like_string: str):
    return like_string.replace("*", r"\*").replace(".", r"\.").replace("\\%", "$$$$$").replace("\\_", "^^^^^").replace("%", ".*").replace("_", ".").replace("$$$$$", "%").replace("^^^^^", "_")

def U_COMP_OP_to_Bool(op, a, b):
    if a == "NULL" or b == "NULL":
        return False
    if op == COMP_OP.EQ_OP:
        return a == b
    if op == COMP_OP.GE_OP:
        return a >= b
    if op == COMP_OP.LE_OP:
        return a <= b
    if op == COMP_OP.GT_OP:
        return a > b
    if op == COMP_OP.LT_OP:
        return a < b
    if op == COMP_OP.NE_OP:
        return a != b