from ..FileSystem.BufPageManager import BufPageManager
from .RM_FileHandle import RM_FileHandle
from ..settings import *
from ..utils import *
import numpy as np
from ..Error import myErr



def getPageHeaderSize(record_size: int):
    '''
    重点是算bitmap的大小。
    '''
    free_size = PAGE_SIZE_BY_BYTE - PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE
    slot_nums = free_size // record_size
    
    while(slot_nums):
        bitmap_size = (slot_nums - 1) // 8 + 1
        if bitmap_size + slot_nums * record_size <= free_size:
            return bitmap_size + PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE
        slot_nums -= 1
    


class RM_Manager:
    def __init__(self, filemanager: BufPageManager):
        self.file_manager = filemanager

    def createFile(self, filename, record_size):
        '''
        record_size是字节为单位的。
        '''
        if record_size > PAGE_SIZE_BY_BYTE - PAGE_HEAD_SIZE_EXCEPT_BITMAP_BY_BYTE - 1: #1是bitmap大小，至少要有1字节的bitmap吧
            raise myErr("IllegalRecordSize", "The record-size is too large.")

        if record_size <= 0:
            raise myErr("IllegalRecordSize", "The record-size is too small.")
        self.file_manager.createFile(filename)
        file_id = self.file_manager.openFile(filename)
        #头页面
        li = [0, 0, 1, getPageHeaderSize(record_size), record_size]
        file_head_data = U_int2byte(li)

        self.file_manager.newPage(file_id, file_head_data)
        self.file_manager.closeFile(filename)

    def openFile(self, file_name):
        '''
        返回一个RM_FileHandle
        '''
        file_id = self.file_manager.openFile(file_name)
        return RM_FileHandle(self.file_manager, file_id, file_name)

    def closeFile(self, file_handle: RM_FileHandle):
        if not file_handle.is_file_open:
            raise myErr("Repeated", "The file is already closed.")

        if file_handle.is_fileheader_changed: #写回第一页
            hdr = file_handle.file_header
            l = [hdr.first_free_page, hdr.num_records, hdr.num_pages, hdr.page_header_size, hdr.per_record_size]
            data = U_int2byte(l)
            self.file_manager.writePage(file_handle.file_id, data, 0)

        self.file_manager.closeFile(file_handle.file_name)

    def removeFile(self, filename):
        try:
            self.file_manager.closeFile(filename)
        except:
            pass
        self.file_manager.removeFile(filename)
