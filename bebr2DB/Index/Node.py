
#基类（页头）：是否叶节点（若非零，则是下一个叶节点的页号；若全1，则是最后一个叶节点）、
# 是否叶节点（若非零，则是上一个叶节点的页号；若全1，则是第一个叶节点）、
# 填充度（几个点是有效的），指的是key
# 父节点的pageid

#需要维持node里的字符串是长度一致的


# 非叶节点：
# key：子节点最大关键字的有序排列
# value：子节点的页号序列 简洁起见，用int32存了
# 注意value是比key要长1的
# key的长度是 attr_length * (m - 1)
# value的长度是 4 * m
#
# 叶节点：
# key：关键字的有序排列
# value：对应的rid序列 简洁起见，写成一个二维数组
# key的长度是 attr_length * (m - 1)
# value的长度是 8 * (m - 1)

import numpy as np
# from ..utils import *
class Node:
    def __init__(self, is_leaf=False, next_leaf_page_id=0, last_leaf_page_id=0, filling_num=0, page_id = 0, parent_id = 0):
        self.is_leaf = is_leaf
        self.next_leaf_page_id = np.int32(next_leaf_page_id)
        self.last_leaf_page_id = np.int32(last_leaf_page_id)
        self.filling_num = filling_num
        self.page_id = page_id
        self.parent_id = np.int32(parent_id)


class InternalNode(Node):
    def __init__(self, key, value, filling_num, page_id, parent_id):
        super().__init__(False, 0, 0, filling_num, page_id, parent_id)
        self.key = key
        self.children_pageid = value

class LeafNode(Node):
    def __init__(self, next_leaf_page_id, last_leaf_page_id, key, value, filling_num, page_id, parent_id):
        super().__init__(True, next_leaf_page_id, last_leaf_page_id, filling_num, page_id, parent_id)
        self.key = key
        self.rid = value

    def isLast(self):
        return self.next_leaf_page_id == -1

    def isFirst(self):
        return self.last_leaf_page_id == -1

class OverflowNode(Node):
    def __init__(self, next_leaf_page_id, last_leaf_page_id, value, filling_num, page_id, parent_id):
        super().__init__(True, next_leaf_page_id, last_leaf_page_id, filling_num, page_id, parent_id)
        self.rid = value

    def isLast(self):
        return self.next_leaf_page_id == -1

    def isFirst(self):
        return self.last_leaf_page_id == -1
