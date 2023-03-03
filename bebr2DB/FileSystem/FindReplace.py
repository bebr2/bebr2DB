#LRU算法

from .LinkList import LinkList
#这里也用到了令牌环，但与hash不同的是只有一个环，因为只需要维护一个被访问的环
class FindReplace:
    def __init__(self, cap):
        self.cap = cap
        self.list = LinkList(cap, 1)
        for i in range(cap):
            self.list.insert(0, i)

    #某个页面缓存回收
    def free(self, index):
        self.list.insertFirst(0, index)

    #某个页面标记为访问
    def access(self, index):
        self.list.insert(0, index)

    def find(self):
        index = self.list.getFirst(0)
        self.list.remove(index)
        self.access(index)
        return index



    
