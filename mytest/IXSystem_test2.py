#测插入重复值的脚本

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
for i in range(120):
    f_handle.insertEntry(i, Rid(1, 3))

for i in range(30):
    f_handle.insertEntry(1, Rid(i+20,2))
for i in range(31):
    f_handle.insertEntry(29, Rid(i+20,2))
for i in range(120):
    f_handle.deleteEntry(i, Rid(1, 2))

scan = IX_Scan()
scan.openScan(f_handle, COMP_OP.NO_OP)

all_count = 0
count1 = 0
count29 = 0
while True:
    a, b, c = scan.getNextEntry()
    if not a:
        break
    all_count += 1
    if b == 1:
        count1 += 1
    if b == 29:
        count29 += 1

print(f'剩{all_count}个数，{all_count == 120+30+31}')
print(f'剩{count1}个1，{count1 == 1+30}')
print(f'剩{count29}个29，{count29 == 1+31}')

scan.closeScan()
scan.openScan(f_handle, COMP_OP.EQ_OP, 1)
count1 = 0
rid_l = []
while True:
    a, b, c = scan.getNextEntry()
    if not a:
        break
    count1 += 1
    rid_l.append(c)
print(f'剩{count1}个1，{count1 == 1+30}')
scan.closeScan()

for rid in rid_l:
    f_handle.deleteEntry(1,rid)

scan.openScan(f_handle, COMP_OP.EQ_OP, 1)
count1 = 0
while True:
    a, b, c = scan.getNextEntry()
    if not a:
        break
    count1 += 1
print(f'剩{count1}个1，{count1 == 0}')
scan.closeScan()

