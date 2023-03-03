from ..bebr2DB.FileSystem import BufPageManager
from ..bebr2DB.Index import IX_FileHandle, IX_Manager, IX_Scan
from ..bebr2DB.RecordManager import Rid
import numpy as np
import struct
from ..bebr2DB.settings import *

bpm = BufPageManager()
ixm = IX_Manager(bpm)

ixm.createIndex("t.txt", 0, ATTR_TYPE.INT, 4)
f_handle = ixm.openIndex("t.txt", 0)
for i in range(120):
    f_handle.insertEntry(i, Rid(1, 2))
print(f_handle.file_header.num_pages)
print(f_handle.root_node.page_id)

for i in range(30):
    f_handle.deleteEntry(i, Rid(1,2))
print(f_handle.file_header.num_pages)

count = 0
for i in range(120):
    a, b, c = f_handle.searchEntry(i)
    if a:
        count += 1

print(f'剩{count}个,{count == 120 - 30}')

for i in range(20):
    f_handle.insertEntry(i, Rid(1, 2))
count = 0
for i in range(120):
    a, b, c = f_handle.searchEntry(i)
    if a:
        count += 1
print(f'剩{count}个,{count == 120 - 30 + 20}')

scan = IX_Scan()
scan.openScan(f_handle, COMP_OP.LT_OP, 30)
for i in range(20):
    a, b, c = scan.getNextEntry()
    print(f'找到了{a}, 这是{b}, {b == 19 - i}')
scan.closeScan()
scan.openScan(f_handle, COMP_OP.NO_OP)
a = True
while True:
    a, b, c = scan.getNextEntry()
    if not a:
        break
    print(b)

scan.closeScan()
scan.openScan(f_handle, COMP_OP.NO_OP)
a = True
while True:
    a, b, c = scan.getNextEntry()
    if not a:
        break
    f_handle.deleteEntry(b, c)

print(f_handle.root_node.filling_num == 0)

ixm.closeIndex(f_handle)
