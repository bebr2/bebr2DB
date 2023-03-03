from ..FileSystem.BufPageManager import BufPageManager
from .IX_FileHandle import IX_FileHandle
from ..settings import *
from ..utils import *
from ..RecordManager import RM_FileHandle
from ..Error import myErr
# import numpy as np

class IX_Manager:
    def __init__(self, filemanager: BufPageManager):
        self.file_manager = filemanager

    def createIndex(self, filename, index_no, attr_type, attr_length):
        if index_no < 0 or filename is None:
            raise myErr("IllegalName", "Failed to create an index.")

        if not U_check_attr_type_and_length(attr_type, attr_length):
            raise myErr("IllegalType", "The Attr len is not consistent with its type.")

        #假设传进来的filename是有RM对应的
        newname = f'{filename}.{index_no}'
        self.file_manager.createFile(newname)
        file_id = self.file_manager.openFile(newname)

        li = [0, attr_type, attr_length, 0, 0]
        file_head_data = U_int2byte(li, INDEX_FILE_HEAD_SIZE_BY_INT_NUM)

        self.file_manager.newPage(file_id, file_head_data)
        self.file_manager.closeFile(newname)

    def openIndex(self, filename, index_no):
        newname = f'{filename}.{index_no}'
        file_id = self.file_manager.openFile(newname)
        return IX_FileHandle(self.file_manager, file_id, newname)

    def closeIndex(self, file_handle: IX_FileHandle):
        if not file_handle.is_file_open:
            raise myErr("Repeated", "The file is already closed.")
        
        if file_handle.is_fileheader_changed: #写回第一页
            hdr = file_handle.file_header
            l = [hdr.root_page_id, hdr.attr_type, hdr.attr_length, hdr.tree_height, hdr.num_pages]
            data = U_int2byte(l)
            self.file_manager.writePage(file_handle.file_id, data, 0)

        self.file_manager.closeFile(file_handle.file_name)

    def removeIndex(self, filename, index_no):
        self.file_manager.removeFile(f'{filename}.{index_no}')