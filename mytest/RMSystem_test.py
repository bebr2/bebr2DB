#需要先把settings里的CAP和MOD都改成60

from ..bebr2DB.FileSystem import BufPageManager
from ..bebr2DB.RecordManager import RM_Scan, RM_Manager, RM_FileHandle, Rid, RM_Record
import numpy as np
import struct
from ..bebr2DB.settings import *


bpm = BufPageManager()
rmm = RM_Manager(bpm)

#先测试 Float和Int，字符串等后面定义到了再说，直至10/28还是只能支持定长记录。
f_id = rmm.createFile("test.txt", 8)
f_handle = rmm.openFile("test.txt")
f_handle.newPage()
f_handle.newPage()
f_handle.newPage()

print(f_handle.file_header.num_pages)
print(f_handle.file_header.per_page_slots_num)

print(f'找到空记录：{f_handle.getRecord(Rid(1, 3)).rid.page_num == -1}')

is_num = 0
insert_num = 0
for i in range(2 * f_handle.file_header.per_page_slots_num):
    int_num = np.frombuffer(np.array([i]), dtype=np.uint8)
    float_num = np.frombuffer(np.array([np.float32(i)]), dtype=np.uint8)
    data = np.append(int_num, float_num)
    f_handle.insertRecord(data)
    if i < 1027:
        is_num += 1
    insert_num += 1

print(f_handle.file_header.num_records)

delete_num = 0
for i in range(f_handle.file_header.per_page_slots_num // 2):
    if 2*i >= f_handle.file_header.per_page_slots_num:
        continue
    f_handle.deleteRecord(Rid(3, 2 * i)) #删掉双数的
    if 2*i < 1027:
        is_num -= 1
    delete_num += 1

d1 = f_handle.getRecord(Rid(2, 10)).data
d2 = f_handle.getRecord(Rid(3, 11)).data

d1_int = struct.unpack('i', d1[:4])[0]
d1_float = struct.unpack('f', d1[4:])[0]
d2_int = struct.unpack('i', d2[:4])[0]
d2_float = struct.unpack('f', d2[4:])[0]

print(f'(1,20)是空记录:{f_handle.getRecord(Rid(3, 20)).rid.page_num == -1}')
print(f'd1_int, d1_float: {d1_int}, {d1_float}. 结果为:{d1_int == f_handle.file_header.per_page_slots_num + 10}')
print(f'd2_int, d2_float: {d2_int}, {d2_float}. 结果为:{d2_int == 11 and d2_float == 11.0}')

scan = RM_Scan()

def comp_fuc(a, b):
    return a < b


scan.openScan(f_handle, ATTR_TYPE.FLOAT, 4, 4, comp_fuc, f_handle.file_header.per_page_slots_num + 20)

rec = RM_Record(None, None)
rec.rid.page_num = 1
count = 0

while rec.rid.page_num != -1:
    rec = scan.getNextRecord(rec)
    count += 1


print(f'符合条件的记录有{count - 1}个，实际期望是{is_num}')

scan.closeScan()
rmm.closeFile(f_handle)

f_h = rmm.openFile("test.txt")
print(f'总记录有{f_h.file_header.num_records}个，实际期望是{insert_num - delete_num}个')


