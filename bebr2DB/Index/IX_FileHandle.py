from ..settings import *
from ..utils import *
from ..FileSystem import BufPageManager
import numpy as np
from .Node import *
from ..RecordManager import Rid
from ..Error import myErr

def computeMlevel(attr_length):
    # return 11
    m_ = (PAGE_SIZE_BY_BYTE - 8 + attr_length) // (8 + attr_length)
    return m_ if m_ % 2 else m_ - 1
class IX_FileHdr:
    '''
    文件头存：
    根节点的id、属性类型、属性长度、树高、页数
    都是4字节，无符号数
    '''
    def __init__(self, root_page_id=0, attr_type=ATTR_TYPE.NONETYPE, attr_length=0, tree_height=0, num_pages=1) -> None:
        self.attr_type = attr_type
        self.attr_length = attr_length
        self.tree_height = tree_height    #可变
        self.root_page_id = root_page_id  #可变
        self.pair_size = attr_length + 8
        self.num_pages = num_pages #head_page算上的 #可变，这里不算溢出页噢
        self.m_level = computeMlevel(attr_length)

    
class IX_FileHandle:
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

        self.empty_page_id_list = []

        arr = self.head_page[:INDEX_FILE_HEAD_SIZE_BY_INT_NUM * 4]
        l = [U_convertTo_uint32(arr[4 * i : 4 * i + 4]) for i in range(5)]

        self.file_header = IX_FileHdr(l[0], l[1], l[2], l[3], l[4])

        if l[4] > 1:
            self.root_node = self.getNode(l[0])
        else:
            typ = int if l[1] == ATTR_TYPE.INT else np.float32 if l[1] == ATTR_TYPE.FLOAT else f'<U{l[2]}'
            key = np.array([], dtype=typ)
            value = np.array([], dtype=int)
            
            self.root_node = LeafNode(-1, -1, key, value, 0, 0, 0)
            self.Node_To_Page(self.root_node)
            self.file_header.root_page_id = self.root_node.page_id
            self.file_header.tree_height = 1
            self.file_header.num_pages += 1
            self.is_fileheader_changed = True

    def checkOverflow(self, rid_array):
        '''
        Rid(-2, first_child_page_id)
        '''
        return (False, None) if rid_array[0] != -2 else (True, rid_array[1])

    
    def searchEntry(self, raw_data):
        '''
        返回的是(是否找到，若找到的叶子节点，位置)
        注意传进来的如果是字符串应该先填满！

        在上溢时parent_page_id是没有修改的，在search时如果判断parent_page_id错了顺便改一下。
        '''
        p = self.root_node
        while not p.is_leaf:
            index_list = np.argwhere(p.key[:p.filling_num] <= raw_data) #这里只是为了用numpy的接口，写成从前到后扫描时间复杂度更低
            child = 0 if len(index_list) == 0 else index_list[-1][0] + 1
            child_page_id = int(p.children_pageid[child])
            q = self.getNode(child_page_id)
            if q.parent_id != p.page_id:
                q.parent_id = p.page_id
                self.Node_To_Page(q)
            p = q

        if p.filling_num == 0:
            return (False, p, 0)
        index_list = np.argwhere(p.key[:p.filling_num] <= raw_data)
        if len(index_list) == 0:
            return (False, p, 0)
        ix = index_list[-1][0]
        va = p.key[ix]
        return (True, p, ix) if va == raw_data else (False, p, ix + 1)

    def searchFirstEntry(self):
        #返回(是否空树，起始叶子，起始index)
        p = self.root_node
        while not p.is_leaf:
            q = self.getNode(p.children_pageid[0])
            if q.parent_id != p.page_id:
                q.parent_id = p.page_id
                self.Node_To_Page(q)
            p = q
        return (False, p, 0) if p.filling_num == 0 else (True, p, 0)


    def insertEntry(self, raw_data, rid: Rid):  # sourcery skip: low-code-quality
        #插入的data是raw_data，索引的顺序是大小
        #注意传进来的如果是字符串，在这里填满！并且保证类型都是对的！
        #TODO
        if self.file_header.attr_type == ATTR_TYPE.STRING:
            raw_data = U_pad_a_string(raw_data, self.file_header.attr_length)
        is_find, leaf_node, ix = self.searchEntry(raw_data)
        if is_find:#插入的是重复值，先判断有没有溢出页
            has_overflow_page, first_page_id = self.checkOverflow(leaf_node.rid[ix])
            if has_overflow_page:
                o_node = self.getOverflowNode(first_page_id)
                while not o_node.isLast():
                    next_page_id = o_node.next_leaf_page_id
                    o_node = self.getOverflowNode(next_page_id)
                if o_node.filling_num < OVERFLOWNODE_MAX_FILLING_NUMS:
                    o_node.filling_num += 1
                    o_node.rid = np.append(o_node.rid, [[rid.page_num, rid.slot_num]], axis=0)
                else:
                    newOverflowNode = OverflowNode(-1, o_node.page_id, np.array([[rid.page_num, rid.slot_num]], dtype=np.int32), 1, 0, leaf_node.page_id)
                    self.OverflowNode_To_Page(newOverflowNode)
                    o_node.next_leaf_page_id = newOverflowNode.page_id
                self.OverflowNode_To_Page(o_node)
            else:
                newOverflowNode = OverflowNode(-1, -1, np.append([leaf_node.rid[ix].copy()], np.array([[rid.page_num, rid.slot_num]], dtype=np.int32), axis=0), 2, 0, leaf_node.page_id)
                self.OverflowNode_To_Page(newOverflowNode)
                leaf_node.rid[ix][0] = -2
                leaf_node.rid[ix][1] = newOverflowNode.page_id
                self.Node_To_Page(leaf_node)

            return True
        m = self.file_header.m_level
        # attr_type = self.file_header.attr_type
        # attr_length = self.file_header.attr_length

        leaf_node.key = np.insert(leaf_node.key, ix, raw_data)

        if leaf_node.filling_num == 0:
            leaf_node.rid = np.array([[rid.page_num, rid.slot_num]], dtype=np.int32)
        else:
            leaf_node.rid = np.insert(leaf_node.rid, ix, np.array([rid.page_num, rid.slot_num]), axis=0)
        leaf_node.filling_num += 1
        if leaf_node.filling_num <= m - 1:
            self.Node_To_Page(leaf_node)
            return True
        else:
            #满了的情况，需要上溢
            self.is_fileheader_changed = True #一定得改头了

            #new a node:
            if self.file_header.tree_height == 1:
                #最早的情况，是特例
                key = leaf_node.key[m//2:m//2+1].copy()
                value = np.array([leaf_node.page_id, 0], dtype=int) #右节点待会填入
                self.root_node = InternalNode(key, value, 1, 0, 0)
                self.Node_To_Page(self.root_node)
                self.file_header.root_page_id = self.root_node.page_id
                leaf_node.parent_id = self.root_node.page_id
            next_ = leaf_node.next_leaf_page_id
            last_ = leaf_node.page_id
            parent_ = leaf_node.parent_id
            key = leaf_node.key[m//2:].copy()
            value = leaf_node.rid[m//2:].copy()
            new_node = LeafNode(next_, last_, key, value, m - m//2, 0, parent_)
            self.Node_To_Page(new_node)
            new_page_id = new_node.page_id
            if not leaf_node.isLast():
                leaf_next_node = self.getNode(next_)
                leaf_next_node.last_leaf_page_id = new_page_id
                self.Node_To_Page(leaf_next_node)
            leaf_node.next_leaf_page_id = new_page_id
            leaf_node.key = leaf_node.key[:m//2]
            leaf_node.rid = leaf_node.rid[:m//2]
            leaf_node.filling_num = m // 2
            self.Node_To_Page(leaf_node)

            if self.file_header.tree_height == 1:
                self.root_node.children_pageid = np.array([leaf_node.page_id, new_page_id], dtype=int)
                self.Node_To_Page(self.root_node)
                self.file_header.tree_height += 1
                return True
            p = leaf_node
            up_data = new_node.key[0]

            #针对父节点的上溢
            while True:
                parent_node = self.getNode(parent_)

                ix = np.argwhere(parent_node.children_pageid == p.page_id)[0][0]
                parent_node.key = np.insert(parent_node.key, ix, up_data)
                parent_node.children_pageid = np.insert(parent_node.children_pageid, ix+1, new_page_id)
                parent_node.filling_num += 1
                if parent_node.page_id == self.root_node.page_id:
                    self.root_node = parent_node
                if parent_node.filling_num <= m - 1:
                    self.Node_To_Page(parent_node)
                    return True
                else:
                    arrive_root = parent_node.page_id == self.root_node.page_id

                    p = parent_node

                    up_data = p.key[m//2]

                    if arrive_root:
                        key = p.key[m//2:m//2+1].copy()
                        value = np.array([p.page_id, 0], dtype=int) #右节点待会填入
                        self.root_node = InternalNode(key, value, 1, 0, 0)
                        self.Node_To_Page(self.root_node)
                        self.file_header.root_page_id = self.root_node.page_id
                        self.file_header.tree_height += 1
                        p.parent_id = self.root_node.page_id
                    parent_ = p.parent_id

                    key = p.key[m//2+1:].copy()
                    value = p.children_pageid[m//2+1:].copy()
                    new_node = InternalNode(key, value, m-m//2-1, 0, parent_)
                    self.Node_To_Page(new_node)
                    new_page_id = new_node.page_id

                    p.key = p.key[:m//2]
                    p.children_pageid = p.children_pageid[:m//2+1]
                    p.filling_num = m // 2
                    self.Node_To_Page(p)

                    if arrive_root:
                        self.file_header.tree_height += 1
                        self.root_node.children_pageid = np.array([p.page_id, new_page_id], dtype=int)
                        self.Node_To_Page(self.root_node)
                        return True



    def deleteEntry(self, raw_data, rid: Rid):  # sourcery skip: low-code-quality
        #注意，这不是B+树真正要求的删除，不会发生节点的重新合并，也就是说，并不要求节点至少要半满
        if self.file_header.attr_type == ATTR_TYPE.STRING:
            raw_data = U_pad_a_string(raw_data, self.file_header.attr_length)
        is_find, leaf_node, ix = self.searchEntry(raw_data)
        if not is_find:
            return False
        has_overflow_page, first_page_id = self.checkOverflow(leaf_node.rid[ix])
        if has_overflow_page:
            #有溢出页，说明不可能把叶子节点删掉，最多是溢出页被删掉，不可能出这个if语句
            o_node = self.getOverflowNode(first_page_id)
            find_the_entry = False
            entry_ix = 0
            while True:
                for _ix, rid_array in enumerate(o_node.rid):
                    if rid_array[0] == rid.page_num and rid_array[1] == rid.slot_num:
                        find_the_entry = True
                        entry_ix = _ix
                        break
                if o_node.isLast() or find_the_entry:
                    break
                next_page_id = o_node.next_leaf_page_id
                o_node = self.getOverflowNode(next_page_id)
            if not find_the_entry:
                return False

            if o_node.isLast():
                if o_node.isFirst() and o_node.filling_num <= 2:
                    reserve_rid = o_node.rid[1-entry_ix]
                    self.empty_page_id_list.append(o_node.page_id)
                    leaf_node.rid[ix] = reserve_rid.copy()
                    self.Node_To_Page(leaf_node)
                    return True
                if o_node.filling_num <= 1:
                    #是最后一页的第一个被删，无所谓，直接删了这一页
                    self.empty_page_id_list.append(o_node.page_id)
                    last_o_node = self.getOverflowNode(o_node.last_leaf_page_id)
                    last_o_node.next_leaf_page_id = -1
                    self.OverflowNode_To_Page(last_o_node)
                    return True
                o_node.rid[entry_ix] = o_node.rid[o_node.filling_num-1].copy()
                o_node.filling_num -= 1
                o_node.rid = o_node.rid[:-1]
                self.OverflowNode_To_Page(o_node)
                return True
            last_o_node = o_node
            while not last_o_node.isLast():
                next_page_id = last_o_node.next_leaf_page_id
                last_o_node = self.getOverflowNode(next_page_id)
            o_node.rid[entry_ix] = last_o_node.rid[last_o_node.filling_num-1].copy()
            self.OverflowNode_To_Page(o_node)
            if last_o_node.filling_num <= 1:
                self.empty_page_id_list.append(last_o_node.page_id)
                now_last_o_node = self.getOverflowNode(last_o_node.last_leaf_page_id)
                now_last_o_node.next_leaf_page_id = -1
                self.OverflowNode_To_Page(now_last_o_node)
            else:
                last_o_node.filling_num -= 1
                last_o_node.rid = last_o_node.rid[:-1]
                self.OverflowNode_To_Page(last_o_node)
            return True
        else:
            if leaf_node.rid[ix][0] != rid.page_num or leaf_node.rid[ix][1] != rid.slot_num:
                return False


        leaf_node.filling_num -= 1
        leaf_node.key = np.delete(leaf_node.key, ix)
        leaf_node.rid = np.delete(leaf_node.rid, ix, axis=0)
        if leaf_node.filling_num > 0 or leaf_node.page_id == self.root_node.page_id:
            self.Node_To_Page(leaf_node)
            return True

        self.is_fileheader_changed = True

        next_ = leaf_node.next_leaf_page_id
        last_ = leaf_node.last_leaf_page_id
        parent_ = leaf_node.parent_id
        if leaf_node.isLast() and not leaf_node.isFirst():
            last_node = self.getNode(last_)
            last_node.next_leaf_page_id = -1
            self.Node_To_Page(last_node)
        if leaf_node.isFirst() and not leaf_node.isLast():
            next_node = self.getNode(next_)
            next_node.last_leaf_page_id = -1
            self.Node_To_Page(next_node)
        if not leaf_node.isFirst() and not leaf_node.isLast():
            last_node = self.getNode(last_)
            last_node.next_leaf_page_id = next_
            self.Node_To_Page(last_node)
            next_node = self.getNode(next_)
            next_node.last_leaf_page_id = last_
            self.Node_To_Page(next_node)
        self.empty_page_id_list.append(leaf_node.page_id)
        self.file_header.num_pages -= 1

        p = self.getNode(parent_)
        q = leaf_node
        while True:
            p.filling_num -= 1
            ix = np.argwhere(p.children_pageid == q.page_id)[0][0]
            p.children_pageid = np.delete(p.children_pageid, ix)
            if p.filling_num >= 0:
                #如果等于0也是可以的，说明现在只有一个，是合法的
                p.key = np.delete(p.key, ix-1) if ix > 0 else np.delete(p.key, 0)
                self.Node_To_Page(p)
                if p.page_id == self.root_node.page_id:
                    self.root_node = p
                return True
            if p.page_id == self.root_node.page_id:
                #删到这里，说明树空了
                typ = int if self.file_header.attr_type == ATTR_TYPE.INT else np.float32 if self.file_header.attr_type == ATTR_TYPE.FLOAT else f'<U{self.file_header.attr_length}'
                key = np.array([], dtype=typ)
                value = np.array([], dtype=int)
                self.root_node = LeafNode(-1, -1, key, value, 0, p.page_id, 0)
                self.Node_To_Page(self.root_node)
                self.file_header.tree_height = 1
                return True


            q = p
            p = self.getNode(p.parent_id)
            self.empty_page_id_list.append(q.page_id)
            self.file_header.num_pages -= 1


    def Page_To_Node(self, page_data : np.ndarray, page_id):
        #将一个页面解包，返回一个节点。
        #注意，并不保证key和value都是m或m-1长的
        #传入的dtype是uint8
        m = self.file_header.m_level
        attr_type = self.file_header.attr_type
        attr_length = self.file_header.attr_length
        page_header = [U_convertTo_uint32(page_data[4 * i : 4 * i + 4]) for i in range(4)]
        #[下一个节点，上一个节点，填充度，父节点]
        if page_header[0] == 0:
            key = U_unpack_attr_array(page_data, attr_type, attr_length, 16, page_header[2])
            value = U_unpack_attr_array(page_data, ATTR_TYPE.INT, 4, 16 + attr_length * (m - 1), page_header[2] + 1)
            return InternalNode(key, value, page_header[2], page_id, page_header[3])
        else:
            key = U_unpack_attr_array(page_data, attr_type, attr_length, 16, page_header[2])
            value = U_unpack_attr_array(page_data, ATTR_TYPE.INT, 4, 16 + attr_length * (m - 1), 2 * page_header[2]).reshape((-1, 2))
            return LeafNode(page_header[0], page_header[1], key, value, page_header[2], page_id, page_header[3])

    def getNode(self, page_id):
        pdata = self.fm.readPage(self.file_id, page_id)
        return self.Page_To_Node(pdata, page_id)
        
    def Node_To_Page(self, node: Node):
        #将一个节点打包，并写进缓存。如果这个节点还没有分配页，那就先在磁盘新建一个页，再写进缓存。
        
        if node.page_id == 0:
            new_data = np.zeros(PAGE_SIZE_BY_BYTE, dtype=np.uint8)
            if len(self.empty_page_id_list) == 0:
                node.page_id = self.fm.newPage(self.file_id, data=new_data)
            else:
                #如果之前有被删的一个页，且文件没关闭过，那就用那个页的ID就行。
                node.page_id = self.empty_page_id_list[-1]
                self.empty_page_id_list = self.empty_page_id_list[:-1]
            self.file_header.num_pages += 1
            self.is_fileheader_changed = True

        m = self.file_header.m_level
        attr_type = self.file_header.attr_type
        attr_length = self.file_header.attr_length
        page = self.fm.getPage(self.file_id, node.page_id)
        if node.is_leaf:
            page[:4] = np.frombuffer(int(node.next_leaf_page_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
            page[4:8] = np.frombuffer(int(node.last_leaf_page_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
            page[8:12] = np.frombuffer(int(node.filling_num).to_bytes(4, byteorder='big'), dtype=np.uint8)
            page[12:16] = np.frombuffer(int(node.parent_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
            page[16:16 + attr_length * (m-1)] = U_pack_attr_array(node.key, attr_type, attr_length, m-1)
            page[16 + attr_length * (m-1):16 + attr_length * (m-1) + 8 * (m-1)] = U_pack_attr_array(node.rid.reshape(-1), ATTR_TYPE.INT, 4, 2*(m-1))
        else:
            page[:8] = np.zeros(dtype=np.uint8, shape=8)
            page[8:12] = np.frombuffer(int(node.filling_num).to_bytes(4, byteorder='big'), dtype=np.uint8) #注意，输入是无符号数
            page[12:16] = np.frombuffer(int(node.parent_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
            page[16:16 + attr_length * (m-1)] = U_pack_attr_array(node.key, attr_type, attr_length, m-1)
            page[16 + attr_length * (m-1):16 + attr_length * (m-1) + 4*m] = U_pack_attr_array(node.children_pageid, ATTR_TYPE.INT, 4, m)

    def OverflowNode_To_Page(self, node:OverflowNode):
        if node.page_id == 0:
            new_data = np.zeros(PAGE_SIZE_BY_BYTE, dtype=np.uint8)
            if len(self.empty_page_id_list) == 0:
                node.page_id = self.fm.newPage(self.file_id, data=new_data)
            else:
                #如果之前有被删的一个页，且文件没关闭过，那就用那个页的ID就行。
                node.page_id = self.empty_page_id_list[-1]
                self.empty_page_id_list = self.empty_page_id_list[:-1]
        page = self.fm.getPage(self.file_id, node.page_id)
        page[:4] = np.frombuffer(int(node.next_leaf_page_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
        page[4:8] = np.frombuffer(int(node.last_leaf_page_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
        page[8:12] = np.frombuffer(int(node.filling_num).to_bytes(4, byteorder='big'), dtype=np.uint8)
        page[12:16] = np.frombuffer(int(node.parent_id).to_bytes(4, byteorder='big', signed=True), dtype=np.uint8)
        page[16:16 + 8 * OVERFLOWNODE_MAX_FILLING_NUMS] = U_pack_attr_array(node.rid.reshape(-1), ATTR_TYPE.INT, 4, 2*OVERFLOWNODE_MAX_FILLING_NUMS)


    def Page_To_OverflowNode(self, page_data, page_id):
        page_header = [U_convertTo_uint32(page_data[4 * i : 4 * i + 4]) for i in range(4)]
        value = U_unpack_attr_array(page_data, ATTR_TYPE.INT, 4, 16, 2 * page_header[2]).reshape((-1, 2))
        return OverflowNode(page_header[0], page_header[1], value, page_header[2], page_id, page_header[3])


    def getOverflowNode(self, page_id):
        pdata = self.fm.readPage(self.file_id, page_id)
        return self.Page_To_OverflowNode(pdata, page_id)