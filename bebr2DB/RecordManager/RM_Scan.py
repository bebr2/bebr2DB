from .Rid import Rid
from .RM_FileHandle import RM_FileHandle
from .RM_Record import RM_Record
from ..utils import *
from ..settings import *
import numpy as np
import struct
from ..Error import myErr

class RM_Scan:
    def __init__(self):
        # self.current = Rid(0, -1)
        self.open = False

    def openScan(self, file_handle:RM_FileHandle, attrType: int, attrLength: int, attrOffset, comp_func, value=None):
        if self.open:
            raise myErr("Repeated", "The scan is already opened.")

        self.file_handle = file_handle
        if not file_handle.is_file_open:
            raise myErr("OpenError", "The file is not opened.")

        if value != None:
            if not U_check_attr_type_and_length(attrType, attrLength):
                raise myErr("IllegalType", "The Attr len is not consistent with its type.")

            if attrOffset < 0 or attrOffset >= self.file_handle.file_header.per_record_size:
                raise myErr("IllegalOffset", "The attr offset is too large or too small.")
        
        #上面是各种判断, value == None就不判断了，直接返回所有东西。

        self.open = True
        self.attr_type = attrType
        self.attr_length = attrLength
        self.attr_offset = attrOffset
        self.comp_func = comp_func
        self.value = value

    def closeScan(self):
        if not self.open:
            raise myErr("Repeated", "The scan is already closed.")
        self.open = False
        

    def comp(self, rec: RM_Record):
        '''
        比较器，假设comp_func在调用前已经初始化
        '''
        if self.value is None:
            return True
        rec_value = rec.data[self.attr_offset: self.attr_offset + self.attr_length]
        rv = U_unpack_attr(rec_value, self.attr_type, self.attr_length)
        return self.comp_func(rv, self.value)

    def getNextRecord(self, rid: Rid):
        '''
        如果要直接拿到第一个record，直接传入
        rec的rid(1, -1)
        '''
        if not self.open:
            raise myErr("OpenError", "The scan is already closed.")

        if not (rid.page_num == 1 and rid.slot_num == -1):
            if not self.file_handle.isValidRid(rid):
                raise myErr("IllegalRid", "You gave an invalid rid.")
        

        for j in range(rid.page_num, self.file_handle.file_header.num_pages):
            page = self.file_handle.fm.readPage(self.file_handle.file_id, j)
            bitmap = np.unpackbits(page[PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE:self.file_handle.file_header.page_header_size]).astype(bool)
            rec_slots = np.where(bitmap == True)[0]
            rec_slots = rec_slots[rec_slots < self.file_handle.file_header.per_page_slots_num]
            if j == rid.page_num:
                rec_slots = rec_slots[rec_slots > rid.slot_num]

            if len(rec_slots) == 0:
                continue
            

            for slot_index in rec_slots:
                rec_ = self.file_handle.getRecord(Rid(j, slot_index)) #这里get到的Record一定是有值的
                if self.comp(rec_):
                    return rec_

        rec_ = RM_Record(None, None)
        return rec_


        

        