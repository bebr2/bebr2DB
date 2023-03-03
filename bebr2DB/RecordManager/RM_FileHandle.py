from .Rid import Rid
from .RM_Record import RM_Record
from ..FileSystem import BufPageManager
from ..utils import *
from ..settings import PAGE_SIZE_BY_BYTE, RECORD_FILE_HEAD_SIZE_BY_INT_NUM, PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE
import numpy as np
from ..Error import myErr

class RM_FileHdr:
    def __init__(self, first_free_page, num_records, num_pages, page_header_size, per_record_size) -> None:
        #都占用4字节
        #size单位都是字节
        self.first_free_page = first_free_page
        self.num_records = num_records
        self.num_pages = num_pages #head_page算上的
        self.page_header_size = page_header_size
        self.per_record_size = per_record_size
        self.per_page_slots_num = (PAGE_SIZE_BY_BYTE - page_header_size) // per_record_size
    
    def getOffset(self, slot_id):
        return self.page_header_size + self.per_record_size * slot_id


class RM_FileHandle:
    '''
    对文件第一页的操作
    '''
    def __init__(self, fm: BufPageManager, file_id, file_name) -> None:
        '''
        传入的file_id必须是打开过的。

        这是针对某一特定文件的
        '''
        self.fm = fm
        self.is_file_open = True
        self.is_fileheader_changed = False
        self.file_id = file_id
        self.head_page = self.fm.readPage(file_id, 0)
        self.file_name = file_name

        arr = self.head_page[:RECORD_FILE_HEAD_SIZE_BY_INT_NUM * 4]
        l = [U_convertTo_uint32(arr[4 * i : 4 * i + 4]) for i in range(5)]
        self.file_header = RM_FileHdr(l[0], l[1], l[2], l[3], l[4])

    # def __del__(self):  # sourcery skip: do-not-use-bare-except
    #     if self.is_file_open:
    #         try:
    #             self.fm.closeFile(self.fm.id_2_filename[self.file_id])
    #         except:
    #             pass
    #         self.is_file_open = False

    def getRecord(self, rid: Rid):
        if not self.isValidRid(rid):
            raise myErr("IllegalRid", "You gave a rid which is not consistent with any record.")
        page = self.fm.readPage(self.file_id, rid.page_num)
        bitmap = np.unpackbits(page[PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE:self.file_header.page_header_size]).astype(bool)
        if not bitmap[rid.slot_num]: #槽为空只能通过bitmap判断:
            return RM_Record(None, None)
        offset = self.file_header.getOffset(rid.slot_num)
        return RM_Record(page[offset: offset + self.file_header.per_record_size], rid)

    def deleteRecord(self, rid: Rid):
        if not self.isValidRid(rid):
            raise myErr("IllegalRid", "You gave a rid which is not consistent with any record.")

        page = self.fm.getPage(self.file_id, rid.page_num)
        bitmap = np.unpackbits(page[PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE:self.file_header.page_header_size]).astype(bool).copy()

        if not bitmap[rid.slot_num]:
            raise myErr("InvalidDelete", "The delete is invalid.")
        
        #改file_header
        self.file_header.num_records -= 1
        self.is_fileheader_changed = True

        #删除，改bitmap
        bitmap[rid.slot_num] = False
        page[PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE:self.file_header.page_header_size] = np.packbits(bitmap)
        
        if self.__getPageNextFree(page) == rid.page_num: #自指，说明原来是满的，需要改
            self.__setPageNextFree(page, self.file_header.first_free_page)
            self.file_header.first_free_page = rid.page_num
        

    def insertRecord(self, data: np.ndarray):
        if data.dtype != np.uint8:
            raise myErr("IllegalArray", "The parameter is illegal.")
        if len(data) != self.file_header.per_record_size:
            raise myErr("LengthError", "The data you inserted is too long or too short.")
        if self.file_header.first_free_page == 0: #说明没有空闲记录页
            self.newPage()
        page_id = self.file_header.first_free_page
        page = self.fm.getPage(self.file_id, page_id)
        bitmap = np.unpackbits(page[PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE:self.file_header.page_header_size]).astype(bool).copy()
        valid_slots = np.where(bitmap == False)[0]
        valid_slots = valid_slots[valid_slots < self.file_header.per_page_slots_num] #注意bitmap的长度不一定等于slot_nums
        to_be_not_free = len(valid_slots) == 1
        valid_slot_id = valid_slots[0]
        bitmap[valid_slot_id] = True
        offset = self.file_header.getOffset(valid_slot_id)

        #插数据，改bitmap
        # print(offset)
        # print(offset + self.file_header.per_record_size)
        # print(data)
        page[offset: offset + self.file_header.per_record_size] = data
        page[4:self.file_header.page_header_size] = np.packbits(bitmap)

        #改file_header
        self.file_header.num_records += 1
        self.is_fileheader_changed = True

        #当前页面是否仍然空闲
        if to_be_not_free:
            self.file_header.first_free_page = self.__getPageNextFree(page)
            self.__setPageNextFree(page, page_id) #不空闲的页面自指

        return Rid(page_id, valid_slot_id)

    def updateRecord(self, record: RM_Record):
        if not self.isValidRid(record.rid):
            raise myErr("IllegalRid", "You gave a rid which is not consistent with any record.")

        page = self.fm.getPage(self.file_id, record.rid.page_num)
        offset = self.file_header.getOffset(record.rid.slot_num)
        page[offset: offset + self.file_header.per_record_size] =record.data

    def newPage(self):
        '''
        新增一个记录页面。
        #TODO
        这个文件的顺序很怪，到底什么时候用令牌环，什么时候从头开始读？
        '''
        new_data = np.zeros(PAGE_SIZE_BY_BYTE, dtype=np.uint8)
        old_first_free = self.file_header.first_free_page
        self.__setPageNextFree(new_data, old_first_free)
        page_id = self.fm.newPage(self.file_id, new_data)
        
        self.file_header.first_free_page = page_id
        self.file_header.num_pages += 1
        self.is_fileheader_changed = True


    def __setPageNextFree(self, data:np.ndarray, next_page_id:int):
        try:
            data[:4] = np.frombuffer(next_page_id.tobytes(), dtype=np.uint8)
        except Exception:
            data[:4] = np.frombuffer(next_page_id.to_bytes(4, byteorder='big'), dtype=np.uint8)

    def __getPageNextFree(self, data:np.ndarray):
        return U_convertTo_uint32(data[:4])

    # def getPageBitmap(self, page_num):
    #     if not self.isValidPageNum(page_num):
    #         raise Exception 

        

    def isValid(self):
        return self.file_header.num_records > 0 if self.is_file_open else False

    def isValidPageNum(self, pagenum) -> bool:
        return self.is_file_open and 0 < pagenum < self.file_header.num_pages

    def isValidRid(self, rid: Rid) -> bool:
        return self.isValidPageNum(rid.page_num) and 0 <= rid.slot_num < self.file_header.per_page_slots_num