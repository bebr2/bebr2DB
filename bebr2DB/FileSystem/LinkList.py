import numpy as np


class LinkList:
    def __init__(self, cap, list_num):
        self.cap = cap
        self.list_num = list_num
        self.a_next = np.arange(self.cap + self.list_num) #多出来的list_num是给hash的头准备的
        self.a_prev = np.arange(self.cap + self.list_num)

    def link(self, prev, next):
        self.a_next[prev] = next 
        self.a_prev[next] = prev

    def remove(self, index):
        if self.a_prev[index] == index:
            return
        self.link(self.a_prev[index], self.a_next[index])
        self.a_prev[index] = index
        self.a_next[index] = index

    def insert(self, listID, ele):
        self.remove(ele)
        node = listID + self.cap
        prev = self.a_prev[node]
        self.link(prev, ele)
        self.link(ele, node)

    def insertFirst(self, listID, ele):
        self.remove(ele)
        node = listID + self.cap
        _next = self.a_next[node]
        self.link(node, ele)
        self.link(ele, _next)

    def getFirst(self, listID):
        return self.a_next[listID + self.cap]

    def next_(self, index):
        return self.a_next[index]

    def isHead(self, index):
        return index >= self.cap

    def isAlone(self, index):
        return self.a_next[index] == index

