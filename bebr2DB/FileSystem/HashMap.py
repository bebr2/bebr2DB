import numpy as np
from .LinkList import LinkList

class HashMap:
    def __init__(self, cap, mod):
        self.cap = cap
        self.mod = mod
        self.list = LinkList(cap, mod)
        self.key1 = np.full(cap, -1, dtype=np.int32)
        self.key2 = np.full(cap, -1, dtype=np.int32)
        self.A = 1
        self.B = 1

    def hash(self, k1, k2):
        return (k1 * self.A + k2 * self.B) % self.mod

    def findIndex(self, k1, k2):
        '''
        根据k1文件号和k2页号查缓存页数组的下标
        '''
        h = self.hash(k1, k2)
        p = self.list.getFirst(h)
        while not self.list.isHead(p):
            if self.key1[p] == k1 and self.key2[p] == k2:
                return p
            p = self.list.next_(p)
        return -1

    def replace(self, index, k1, k2):
        h = self.hash(k1, k2)
        self.list.insertFirst(h, index)
        self.key1[index] = k1
        self.key2[index] = k2

    def remove(self, index):
        self.list.remove(index)
        self.key1[index] = -1
        self.key2[index] = -1

    def getKeys(self, index):
        return (self.key1[index], self.key2[index])