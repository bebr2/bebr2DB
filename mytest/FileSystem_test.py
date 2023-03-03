#需要先把settings里的CAP和MOD都改成60

import traceback
from ..bebr2DB.FileSystem import BufPageManager
import numpy as np
bpm = BufPageManager()

bpm.createFile("test1.txt")
bpm.createFile("test2.txt")
f1 = bpm.openFile("test1.txt")
f2 = bpm.openFile("test2.txt")


for _ in range(100):
    bpm.newPage(f1, np.zeros(8192 >> 2, dtype=np.uint8))
    bpm.newPage(f2, np.zeros(8192 >> 2, dtype=np.uint8))

for i in range(100):
    bf1 = bpm.getPage(f1, i)
    bf2 = bpm.getPage(f2, i)

    bf1[0] = 1
    bf2[0] = 2


for i in range(100):
    bf1 = bpm.readPage(f1, i)
    bf2 = bpm.readPage(f2, i)

print(bpm.filename_2_id)
print(bpm.file_pages_in_buffer)

#检测replace算法对不对
for i in range(3, 63):
    bpm.readPage(f2, i)
a, b = bpm.getKey(bpm.replace.list.getFirst(0))
print(a == f2)
print(b == 3)
bpm.closeFile("test1.txt")
try:
    bpm.closeFile("test1.txt")
except Exception as e:
    print("----------------------------------")
    traceback.print_exc()
    print("----------------------------------")
bpm.close()

#计算一下有多少个1，看对不对

import os

try:
    OS_OPEN_MODE = os.O_RDWR | os.O_BINARY
except Exception:
    OS_OPEN_MODE = os.O_RDWR

count = 0

for i in range(100):
    fileID = os.open("test1.txt", OS_OPEN_MODE)
    offset = i << 13
    os.lseek(fileID, offset, os.SEEK_SET)
    data = os.read(fileID, 8192)           #需要在内存中读一下
    data = np.frombuffer(data, np.uint8, 8192 >> 2)
    if data[0] == 1:
        count += 1

print(count)
print(count == 100)