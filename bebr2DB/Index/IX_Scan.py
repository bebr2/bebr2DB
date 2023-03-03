from .IX_FileHandle import IX_FileHandle
from ..RecordManager import Rid
from ..utils import *
from ..settings import *
import numpy as np
import struct
from ..Error import myErr

class IX_Scan:
    def __init__(self):
        self.open = False

    def closeScan(self):
        if not self.open:
            raise myErr("Repeated", "The scan is already closed.")
        self.open = False

    def openScan(self, file_handle:IX_FileHandle, compOP: int, value = None):
        # sourcery skip: merge-comparisons, merge-duplicate-blocks, remove-pass-elif, remove-redundant-if
        #对于B+树，禁止NE_OP比较符
        #就不比较了，默认调用正确
        if self.open:
            raise myErr("Repeated", "The scan is already opened.")
        if compOP == COMP_OP.NE_OP:
            raise myErr("IllegalType", "Index scan not allow the no_equal operator.")
        self.open = True
        self.compOP = compOP
        self.value = value
        self.f_handle = file_handle
        if isinstance(self.value, str):
            length = self.f_handle.file_header.attr_length
            self.value = U_pad_a_string(self.value, length)
        self.first_use = True

        if compOP != COMP_OP.NO_OP:
            self.is_find, self.leaf_node, self.ix = self.f_handle.searchEntry(self.value)
        else:
            self.is_find, self.leaf_node, self.ix = self.f_handle.searchFirstEntry()

        if compOP == COMP_OP.GT_OP:
            if self.is_find:
                self.ix += 1
        elif compOP == COMP_OP.LT_OP:
            self.ix -= 1
        elif compOP == COMP_OP.LE_OP:
            if not self.is_find:
                self.ix -= 1

        self.the_leaf_node_has_overflow_page = False

    def getNextEntry(self):
        '''
        一旦找不到数据，就必须关闭，不能再get下去。
        '''
        if not self.open:
            raise myErr("Repeated", "The scan is already closed.")
        if self.compOP == COMP_OP.EQ_OP:
            return self.__getNextEntry()

        if self.first_use or self.the_leaf_node_has_overflow_page == False:
            canfind, value, rid = self.__getNextEntry()
            self.last_value = value
            if not canfind:
                return (False, None, None)
            self.first_use = False
            if rid.page_num == -2:
                self.the_leaf_node_has_overflow_page = True
                self.overflow_node = self.f_handle.getOverflowNode(rid.slot_num)
                self.entry_ix = 1
                return (True, value, Rid(self.overflow_node.rid[0][0], self.overflow_node.rid[0][1]))
            else:
                self.the_leaf_node_has_overflow_page = False
                return (canfind, value, rid)

        if self.entry_ix < self.overflow_node.filling_num:
            self.entry_ix += 1
            return (True, self.last_value, Rid(self.overflow_node.rid[self.entry_ix-1][0], self.overflow_node.rid[self.entry_ix-1][1]))
        else:
            if self.overflow_node.isLast():
                self.the_leaf_node_has_overflow_page = False
                return self.getNextEntry()
            self.overflow_node = self.f_handle.getOverflowNode(self.overflow_node.next_leaf_page_id)
            self.entry_ix = 1
            return (True, self.last_value, Rid(self.overflow_node.rid[0][0], self.overflow_node.rid[0][1]))

        
        # self.the_leaf_node_has_overflow_page, first_page_id = self.checkOverflow(self.leaf_node.rid[self.ix])

                

    def __getNextEntry(self):
        '''

        取消了tree_changed的设计，要保证在openscan的期间不要修改B+树的内容

        如果Scan太久，可以尝试先close，删除一波再打开，以节省内存

        返回值为（是否找到，索引值，Rid值）
        '''
        if self.compOP == COMP_OP.EQ_OP:
            #ix是不会改的
            if not self.is_find:
                return (False, None, None)
            has_overflow_page, first_page_id = self.f_handle.checkOverflow(self.leaf_node.rid[self.ix])
            if not has_overflow_page:#说明无重复值
                if not self.first_use:
                    return (False, None, None)
                self.first_use = False
                return (True, self.value, Rid(self.leaf_node.rid[self.ix][0], self.leaf_node.rid[self.ix][1]))

            if self.first_use:
                self.first_use = False
                self.overflow_node = self.f_handle.getOverflowNode(first_page_id)
                self.entry_ix = 1
                return (True, self.value, Rid(self.overflow_node.rid[0][0], self.overflow_node.rid[0][1]))

            if self.entry_ix < self.overflow_node.filling_num:
                self.entry_ix += 1
                return (True, self.value, Rid(self.overflow_node.rid[self.entry_ix-1][0], self.overflow_node.rid[self.entry_ix-1][1]))
            else:
                if self.overflow_node.isLast():
                    return (False, None, None)
                self.overflow_node = self.f_handle.getOverflowNode(self.overflow_node.next_leaf_page_id)
                self.entry_ix = 1
                return (True, self.value, Rid(self.overflow_node.rid[0][0], self.overflow_node.rid[0][1]))


        # self.first_use = False


        if self.compOP in [COMP_OP.NO_OP, COMP_OP.GE_OP, COMP_OP.GT_OP]:
            if 0 <= self.ix < self.leaf_node.filling_num:
                self.ix += 1
                return (True, self.leaf_node.key[self.ix-1], Rid(self.leaf_node.rid[self.ix-1][0], self.leaf_node.rid[self.ix-1][1]))
            else:
                if self.leaf_node.isLast():
                    return (False, None, None)

                p = self.leaf_node
                while not p.isLast():
                    p = self.f_handle.getNode(p.next_leaf_page_id)
                    if p.filling_num > 0:
                        self.ix = 1
                        self.leaf_node = p
                        return (True, p.key[self.ix-1], Rid(p.rid[self.ix-1][0], p.rid[self.ix-1][1]))
                if p.filling_num > 0:
                    self.ix = 1
                    self.leaf_node = p
                    return (True, p.key[self.ix-1], Rid(p.rid[self.ix-1][0], p.rid[self.ix-1][1]))
                return (False, None, None)
        elif self.compOP in [COMP_OP.LE_OP, COMP_OP.LT_OP]:
            if 0 <= self.ix < self.leaf_node.filling_num:
                self.ix -= 1
                return (True, self.leaf_node.key[self.ix+1], Rid(self.leaf_node.rid[self.ix+1][0], self.leaf_node.rid[self.ix+1][1]))
            else:
                if self.leaf_node.isFirst():
                    return (False, None, None)
                p = self.leaf_node
                while not p.isFirst():
                    p = self.f_handle.getNode(p.last_leaf_page_id)
                    if p.filling_num > 0:
                        self.ix = p.filling_num - 2
                        self.leaf_node = p
                        return (True, p.key[self.ix+1], Rid(p.rid[self.ix+1][0], p.rid[self.ix+1][1]))
                if p.filling_num > 0:
                    self.ix = p.filling_num - 2
                    self.leaf_node = p
                    return (True, p.key[self.ix+1], Rid(p.rid[self.ix+1][0], p.rid[self.ix+1][1]))
                return (False, None, None)
        return (False, None, None)
        


