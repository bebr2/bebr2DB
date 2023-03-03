import numpy as np
from .HashMap import HashMap
from .FindReplace import FindReplace
from ..settings import *
from ..Error.FileSystem import *
import os
from ..Error import myErr

class BufPageManager:
    def __init__(self):
        self.last = -1                                  #这个到底是啥？
        self.dirty = np.full(CAP, False)
        self.buffer = np.zeros((CAP, PAGE_SIZE_BY_BYTE), dtype=np.uint8)
        self.hash = HashMap(CAP, MOD)
        self.replace = FindReplace(CAP)
        self.filename_2_id = {}        #这里有点问题，一个文件可以被打开多次，且id不同
        self.id_2_filename = {}
        self.file_pages_in_buffer = {} #key: fileID; value: set of page index
 
        
        

    def __fetchPage(self, fileID, pageID):
        '''
        通过替换算法获取一个缓存页面的下标，（如果是脏页）写回内存。
        返回缓存下标。
        '''
        new_index = self.replace.find()
        buf = self.buffer[new_index]
        if self.dirty[new_index] and not self.isEmptyPage(new_index):
            old_fileID, old_pageID = self.hash.getKeys(new_index)
            offset = old_pageID << PAGE_SIZE_IDX
            os.lseek(old_fileID, offset, os.SEEK_SET) #相对文件起始位置偏移
            os.write(old_fileID, buf.tobytes())
            self.file_pages_in_buffer[old_fileID].remove(new_index)
            self.dirty[new_index] = False


        self.hash.replace(new_index, fileID, pageID)

        return new_index

    def __getPage(self, fileID, pageID):                        #外部不要调用这个函数直接读
        if fileID not in self.id_2_filename:
            raise OpenFileFailed(f"Can't get the file which id is {fileID}.")
        index = self.hash.findIndex(fileID, pageID)
        if index != -1:                                #这个页面在缓存中
            self.access(index)
            return self.buffer[index], index
        else:                                                   #不在缓存中，分配一个缓存页面
            new_index = self.__fetchPage(fileID, pageID)
            # print(pageID)
            offset = pageID << PAGE_SIZE_IDX
            os.lseek(fileID, offset, os.SEEK_SET)
            data = os.read(fileID, PAGE_SIZE_BY_BYTE)           #需要在内存中读一下
            data = np.frombuffer(data, np.uint8, PAGE_SIZE_BY_BYTE)
            self.buffer[new_index] = data                       #并储存在缓存中
            self.file_pages_in_buffer[fileID].add(new_index)

            
            return self.buffer[new_index], new_index

    def readPage(self, fileID, pageID):
        return self.__getPage(fileID, pageID)[0].copy()

    def getPage(self, fileID, pageID):                          #默认调用这个函数就是要写了，在函数外写只能改变元素而不是整个改变
        page, index = self.__getPage(fileID, pageID)
        self.markDirty(index)
        return page

    def newPage(self, fileID, data):                                 #纯IO操作，慎重调用
        # print(self.filename_2_id)
        pos = os.lseek(fileID, 0, os.SEEK_END)
        os.write(fileID, data.tobytes())
        return pos >> PAGE_SIZE_IDX

    def writePage(self, fileID, data, page_id):                              #纯IO操作的，慎重调用
        offset = page_id << PAGE_SIZE_IDX
        pos = os.lseek(fileID, offset, os.SEEK_SET)
        os.write(fileID, data.tobytes())
        return pos >> PAGE_SIZE_IDX

    def getPageNum(self, fileID):                                  #返回总页数
        pos = os.lseek(fileID, 0, os.SEEK_END)
        return ((pos - 1) >> PAGE_SIZE_IDX) + 1

    def access(self, index):
        if index == self.last:
            return
        self.replace.access(index)
        self.last = index

    def markDirty(self, index):
        self.dirty[index] = True
        self.access(index)

    def release(self, index):
        fileID, pageID = self.getKey(index)
        if fileID == -1:
            return
        self.file_pages_in_buffer[fileID].remove(index)
        self.dirty[index] = False
        self.replace.free(index)
        self.hash.remove(index)
        

    def writeBack(self, index): #注意：没有处理file_pages_in_buffer
        if self.dirty[index]:
            fileID, pageID = self.hash.getKeys(index)
            offset = pageID << PAGE_SIZE_IDX
            os.lseek(fileID, offset, os.SEEK_SET) #相对文件起始位置偏移
            os.write(fileID, self.buffer[index].tobytes())
            self.dirty[index] = False
        self.replace.free(index)
        self.hash.remove(index)

    def close(self):
        for i in range(CAP):
            self.writeBack(i)

    def getKey(self, index):
        '''
        返回缓存index页对应的fileID, pageID
        '''
        return self.hash.getKeys(index)

    def isEmptyPage(self, index):
        return self.getKey(index)[0] == -1

    def createFile(self, filename: str):
        open(filename, 'w').close()

    def openFile(self, filename: str) -> int:
        if filename in self.filename_2_id:
            return self.filename_2_id[filename]
        '''
        应该允许重复打开同一个文件的？还没处理这件事情。#TODO
        现在是不允许重复打开
        '''
        try:
            OS_OPEN_MODE = os.O_RDWR | os.O_BINARY
        except Exception:
            OS_OPEN_MODE = os.O_RDWR
        fileID = os.open(filename, OS_OPEN_MODE)
        if fileID == -1:
            raise OpenFileFailed(f"Can't find the file {filename}.")
        self.filename_2_id[filename] = fileID
        self.id_2_filename[fileID] = filename
        self.file_pages_in_buffer[fileID] = set()
        # print(self.filename_2_id)
        return fileID

    def closeFile(self, filename: str):
        if filename not in self.filename_2_id:
            raise CloseFileFailed(f'The file {filename} is not opening.')
        fileID = self.filename_2_id[filename]
        indexes = self.file_pages_in_buffer.pop(fileID)
        for index in indexes:
            self.writeBack(index)
        os.close(fileID)
        self.filename_2_id.pop(filename)
        self.id_2_filename.pop(fileID)

    def removeFile(self, filename: str):
        if filename in self.filename_2_id:
            raise RemoveFileFailed(f"Please close the file {filename} first.")
        if not self.existFile(filename):
            raise RemoveFileFailed(f"Can't find the file {filename}.")
        os.remove(filename)

    def existFile(self, filename: str):
        return os.path.exists(filename)

