import contextlib
from ..FileSystem import BufPageManager
from ..Index import IX_Manager, IX_Scan
from ..RecordManager import RM_Manager, RM_Scan, Rid, RM_Record
from ..settings import *
from ..utils import *
import os
from pathlib import Path
import shutil
from ..Error import myErr
import re



'''
relcat表还没想好实现，所以先不加东西
'''

class SM_Manager:
    def __init__(self, ixm:IX_Manager, rmm:RM_Manager, bpm:BufPageManager) -> None:
        self.bpm = bpm
        self.ixm = ixm
        self.rmm = rmm
        self.open = False
        self.database_dir_path = os.path.join(Path(os.path.abspath(os.path.dirname(__file__))).parent, "DataBase")
        self.attr_fhandle = None
        self.rel_fhandle = None
        self.dbName = None
        self.rm_scan = RM_Scan()
        self.ix_scan = IX_Scan()

    def check_db_open(f):
        def decorated(self, *args, **kwargs):
            if not self.open:
                raise myErr("OpenError", "You have not open any DB.")
            return f(self, *args, **kwargs)
        return decorated

    def getDbPath(self):
        return os.path.join(self.database_dir_path, self.dbName)

    def getDbTablePath(self, tableName):
        return os.path.join(self.database_dir_path, self.dbName, tableName)

    def getDbAttrcatPath(self):
        return self.getDbTablePath("attrcat")

    def getDbRelcatPath(self):
        return self.getDbTablePath("relcat")

    def getAllTableName(self):
        #会把.*结尾的都视为索引文件
        path = self.getDbPath()
        name_list = os.listdir(path)
        with contextlib.suppress(Exception):
            name_list = list(set(name_list) - {'relcat', 'attrcat'})
        index_list = [name for name in name_list if name.find(".") != -1]
        with contextlib.suppress(Exception):
            name_list = list(set(name_list) - set(index_list))
        return name_list

    def getAllDbName(self):
        return os.listdir(self.database_dir_path)
    
    def getAllDbandTableName(self):
        #TODO
        #偷个懒，会把.*结尾的都视为索引文件，这个函数只让js调用，因为不是很准确
        db_list = self.getAllDbName()
        tb_lists = []
        save = self.dbName
        for db in db_list:
            self.dbName = db
            tb_lists.append(self.getAllTableName())
        self.dbName = save
        return [db_list, tb_lists]
    

    def CreateDb(self, dbName):
        db_list = self.getAllDbName()
        if dbName in db_list:
            raise myErr("Repeated", f"The DB {dbName} already exists.")
        if len(dbName.encode("utf-8")) > MAX_NAME_LEN or dbName == "":
            raise Exception
        save_dbname = self.dbName
        self.dbName = dbName
        os.mkdir(self.getDbPath())
        self.rmm.createFile(self.getDbAttrcatPath(), ATTRCAT_RECORD_SIZE)
        self.dbName = save_dbname
        # self.rmm.createFile(self.getDbRe

    def DropDb(self, dbName):
        db_list = self.getAllDbName()
        if dbName not in db_list:
            raise myErr("DropError", "You have dropped a wrong DB.")
        if self.open and self.dbName == dbName:
            self.CloseDb()
        # print(os.path.join(self.database_dir_path, dbName))
        shutil.rmtree(os.path.join(self.database_dir_path, dbName))
        

    def ShowDbs(self):
        return self.getAllDbName()
    
    def OpenDb(self, dbName):
        if dbName == "" or dbName is None:
            raise myErr("IllegalName", "The DB name is illegal.")
        if self.open and self.dbName == dbName:
            return
        
        save_dbname = self.dbName
        self.dbName = dbName
        if not os.path.exists(self.getDbPath()):
            self.dbName = save_dbname
            raise myErr("OpenError", f"There isn't a DB named {dbName}.")
        self.dbName = save_dbname
        if self.open:
            self.CloseDb()
        self.dbName = dbName
        
        self.attr_fhandle = self.rmm.openFile(self.getDbAttrcatPath())
        # self.rel_fhandle = self.rmm.openFile(self.getDbRelcatPath())
        self.open = True


    @check_db_open
    def CloseDb(self):
        path = self.getDbPath()
        name_list = os.listdir(path)
        self.rmm.closeFile(self.attr_fhandle)
        for name in name_list:
            try:
                self.bpm.closeFile(self.getDbTablePath(name))
            except Exception:
                pass
        self.open = False
        self.dbName = ""

    @check_db_open
    def CreateTable(self, relName, attrInfos, primary_key = None, fk_list = None):
        #attrInfo是list，元素是字典，key是name，type，length, null, default, default_is_null, unique, have_fk, foreign_table, foreign_key_name, rank
        if relName == "" or attrInfos is None or len(attrInfos) == 0:
            raise myErr("IllegalName", "Illegal table name or attr name.")
        attrCount = len(attrInfos)
        if attrCount > MAX_ATTR_COUNT:
            raise myErr("TooMuch", "Too many attr here.")
        if relName in ["relcat", "attrcat"] or len(relName.encode('utf-8')) > MAX_NAME_LEN:
            raise myErr("IllegalName", "The table name is not allowed or too long.")
        if os.path.exists(self.getDbTablePath(relName)):
            raise myErr("Repeated", "The table already exists.")


        unique_relName_set = set()
        for attrInfo in attrInfos:
            if len(attrInfo["name"].encode('utf-8')) > MAX_NAME_LEN:
                raise myErr("TooLong", "The attr name is too long.")
            if attrInfo["name"] in unique_relName_set:
                raise myErr("Repeated", "There are two same attr names.")
            unique_relName_set.add(attrInfo["name"])

        if primary_key is not None and primary_key not in unique_relName_set:
            raise myErr("PKError", "The primary key is not in the attr pool.")

        #[fk_name, main_table, main_column, vice_column]
        fk_name_list = []

        if fk_list is not None and len(fk_list) > 0:
            for fk in fk_list:
                fk_name, main_table, main_column, vice_column = fk
                if main_table == relName:
                    raise myErr("FKError", "You can't create foreign key on itself.")
                if len(fk_name.encode('utf-8')) > MAX_NAME_LEN:
                    raise myErr("IllegalName", "The foreign key name is not allowed or too long.")
                if fk_name.find(".") != -1:
                    raise myErr("FKError", "Can't support foreign key name with '.'.")
                if vice_column not in unique_relName_set:
                    raise myErr("FKError", "The foreign key is not in the attr pool.")
                if not os.path.exists(self.getDbTablePath(main_table)):
                    raise myErr("TableError", f"There isn't a table named {main_table}.")
                exist, rid, info = self.getAttrInfoFromCat(main_table, main_column)
                if not exist:
                    raise myErr("AttrError", f"There isn't an attr named {main_column} on {main_table}.")
                if info["indexNo"] != 0:
                    raise myErr("FKError", "The attr be referred should be a primary key.")
                for attrInfo in attrInfos:
                    if attrInfo["name"] == vice_column:
                        if attrInfo["type"] != info["attrType"]:
                            raise myErr("FKError", "The foreign keys' type must be the same.")
                        break
                if fk_name != "":
                    if fk_name in fk_name_list:
                        raise myErr("FKError", f"The foreign key name {fk_name} already exists.")
                    fk_name_list.append(fk_name)

        #判断fk_name是否重复
        fk_name_list
        # if fk_name_list:
        #     alltable = self.getAllTableName()
        #     for table in alltable:
        #         infos_ = self.getAllAttrInfoFromCat(table)
        #         for info in infos_:
        #             if info["have_fk"] == 1:
        #                 fkn = info["foreign_key_name"]
        #                 fkn = fkn[:fkn.find(".")]
        #                 if fkn != "" and fkn in fk_name_list:
        #                     raise myErr("FKError", "The foreign key name already exists.")

        size_ = (len(attrInfos) - 1) // 8 + 1  #用于记录NULL的
        for i, attrInfo in enumerate(attrInfos):
            #TODO 一点一点来加
            have_fk = 0
            fk_name, main_table, main_column, vice_column = "a", "a", "a", "a"
            for fk in fk_list:
                if fk[3] == attrInfo["name"]:
                    have_fk = 1
                    fk_name, main_table, main_column, vice_column = fk
                    break
            ft = main_table
            fn = f"{fk_name}.{main_column}"
            dataAttrInfo = {"indexNo":-1, "relName": relName, "attrName": attrInfo["name"], 
            "attrType": attrInfo["type"], "attrLength": attrInfo["length"], "offset": size_,
            "null": 1 if attrInfo["null"] else 0, "default": 0 if attrInfo["default"] is None else attrInfo["default"], "default_is_null": 1 if attrInfo["default_is_null"] else 0,
            "unique": 0, "have_fk": have_fk, "foreign_table": ft, "foreign_key_name": fn, "rank":i}
            size_ += attrInfo["length"]
            # print(dataAttrInfo)
            # print(len(U_pack_attrcat_data(dataAttrInfo)))
            self.attr_fhandle.insertRecord(U_pack_attrcat_data(dataAttrInfo))

        self.rmm.createFile(self.getDbTablePath(relName), size_)
        if primary_key is not None:
            self.AddPK(relName, primary_key, False)
        # dataRelInfo = {"relName": relName, "recordLength": size_}
        #TODO 要记录其他信息再在这里面记录，现在觉得relcat不太必要
    
    @check_db_open
    def DropFK(self, vice_table, fk_name):
        if not os.path.exists(self.getDbTablePath(vice_table)):
            raise myErr("TableError", f"There isn't a table named {vice_table}.")
        attrInfos = self.getAllAttrInfoFromCat(vice_table)
        need_to_delete_info = None
        for attrinfo in attrInfos:
            if attrinfo["have_fk"] == 1:
                fkn = attrinfo["foreign_key_name"]
                fkn = fkn[:fkn.find(".")]
                if fkn == fk_name:
                    need_to_delete_info = attrinfo
                    break
        if need_to_delete_info is None:
            raise myErr("DropFKError", f"There isn't a foreign key named {fk_name}")
        exist, rid, info = self.getAttrInfoFromCat(vice_table, need_to_delete_info["attrName"])
        info["have_fk"] = 0
        record = RM_Record(U_pack_attrcat_data(info), rid)
        self.attr_fhandle.updateRecord(record)

    @check_db_open
    def AddFK(self, main_table, main_column, vice_table, vice_column, fkname):
        if len(fkname.encode('utf-8')) > MAX_NAME_LEN:
            raise myErr("IllegalName", "The foreign key name is not allowed or too long.")
        if fkname.find(".") != -1:
            raise myErr("FKError", "Can't support foreign key name with '.'.")
        if main_table == vice_table:
            raise myErr("FKError", "You can't create foreign key on itself.")
        if not os.path.exists(self.getDbTablePath(main_table)):
            raise myErr("TableError", f"There isn't a table named {main_table}.")
        exist, rid, info = self.getAttrInfoFromCat(main_table, main_column)
        if not exist:
            raise myErr("AttrError", f"There isn't an attr named {main_column} on {main_table}.")
        if info["indexNo"] != 0:
            raise myErr("FKError", "The attr be referred should be a primary key.")
        type1 = info["attrType"]

        if not os.path.exists(self.getDbTablePath(vice_table)):
            raise myErr("TableError", f"There isn't a table named {vice_table}.")
        exist, rid, info = self.getAttrInfoFromCat(vice_table, vice_column)
        if not exist:
            raise myErr("AttrError", f"There isn't an attr named {vice_column} on {vice_table}.")
        if type1 != info["attrType"]:
            raise myErr("FKError", "The foreign keys' type must be the same.")

        if fkname != "":
            infos_ = self.getAllAttrInfoFromCat(vice_table)
            for info_ in infos_:
                if info_["have_fk"] == 1:
                    fkn = info_["foreign_key_name"]
                    fkn = fkn[:fkn.find(".")]
                    if fkn != "" and fkn == fkname:
                        raise myErr("FKError", "The foreign key name already exists.")
        
        
        info["have_fk"] = 1
        info["foreign_table"] = main_table
        info["foreign_key_name"] = f"{fkname}.{main_column}"
        record = RM_Record(U_pack_attrcat_data(info), rid)
        self.attr_fhandle.updateRecord(record)
    

    @check_db_open
    def DropTable(self, relName):
        table_list = self.getAllTableName()
        if relName not in table_list:
            raise myErr("DropError", "The table doesn't exist.")


        self.rm_scan.openScan(self.attr_fhandle, ATTR_TYPE.STRING, MAX_NAME_LEN, 0, STRING_EQUAL_FUNC, relName)

        rid = Rid(1, -1)

        index_list = []
        rid_list = []

        while True:
            record = self.rm_scan.getNextRecord(rid)
            rid = record.rid
            if rid == Rid(-1, -1):
                break
            attr_index_no = U_unpack_attr(record.data[2*MAX_NAME_LEN+12:2*MAX_NAME_LEN+16], ATTR_TYPE.INT)
            if attr_index_no != -1:
                attr_name = U_unpack_attr(record.data[MAX_NAME_LEN:2*MAX_NAME_LEN], ATTR_TYPE.STRING, MAX_NAME_LEN)
                index_list.append(attr_name)
            rid_list.append(rid)

        self.rm_scan.closeScan()
        for a in index_list:
            self.DropIndex(relName, a, True)
        for rid in rid_list:
            self.attr_fhandle.deleteRecord(rid)
        self.rmm.removeFile(self.getDbTablePath(relName))

    def getAttrInfoFromCat(self, relName, attrName:str):
        table_list = self.getAllTableName()
        if relName not in table_list:
            raise myErr("OpenError", "The table doesn't exist.")
        self.rm_scan.openScan(self.attr_fhandle, ATTR_TYPE.STRING, MAX_NAME_LEN, 0, STRING_EQUAL_FUNC, relName)
        rid = Rid(1, -1)
        while True:
            record = self.rm_scan.getNextRecord(rid)
            rid = record.rid
            if rid == Rid(-1, -1):
                break
            attr_name = U_unpack_attr(record.data[MAX_NAME_LEN:2*MAX_NAME_LEN], ATTR_TYPE.STRING, MAX_NAME_LEN)
            if attr_name == attrName.rstrip():
                ofs = U_unpack_attr(record.data[2*MAX_NAME_LEN:2*MAX_NAME_LEN+4], ATTR_TYPE.INT)
                aty = U_unpack_attr(record.data[2*MAX_NAME_LEN+4:2*MAX_NAME_LEN+8], ATTR_TYPE.INT)
                alh = U_unpack_attr(record.data[2*MAX_NAME_LEN+8:2*MAX_NAME_LEN+12], ATTR_TYPE.INT)
                ino = U_unpack_attr(record.data[2*MAX_NAME_LEN+12:2*MAX_NAME_LEN+16], ATTR_TYPE.INT)
                #null, default, default_is_null, unique, have_fk, foreign_table, foreign_key_name
                null = U_unpack_attr(record.data[2*MAX_NAME_LEN+16:2*MAX_NAME_LEN+20], ATTR_TYPE.INT)
                din = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+20:2*MAX_NAME_LEN+MAX_STRING_LEN+24], ATTR_TYPE.INT)
                unique = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+24:2*MAX_NAME_LEN+MAX_STRING_LEN+28], ATTR_TYPE.INT)
                have = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+28:2*MAX_NAME_LEN+MAX_STRING_LEN+32], ATTR_TYPE.INT)
                ft = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32:2*MAX_NAME_LEN+MAX_STRING_LEN+32+MAX_NAME_LEN], ATTR_TYPE.STRING, MAX_NAME_LEN).rstrip()
                fn = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32+MAX_NAME_LEN:2*MAX_NAME_LEN+MAX_STRING_LEN+32+3*MAX_NAME_LEN], ATTR_TYPE.STRING, 2*MAX_NAME_LEN).rstrip()
                rank = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32+3*MAX_NAME_LEN:2*MAX_NAME_LEN+MAX_STRING_LEN+36+3*MAX_NAME_LEN], ATTR_TYPE.INT)

                if aty == ATTR_TYPE.INT:
                    default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+24], ATTR_TYPE.INT)
                elif aty == ATTR_TYPE.FLOAT:
                    default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+24], ATTR_TYPE.FLOAT)
                else:
                    default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+MAX_STRING_LEN+20], ATTR_TYPE.STRING, MAX_STRING_LEN).rstrip()
                
                
                self.rm_scan.closeScan()
                return (True, rid, {'relName':relName, 'attrName':attrName, 'offset': ofs, 'attrType': aty, 'attrLength': alh, 'indexNo': ino,
                'null':null, 'default':default, 'default_is_null':din, 'unique':unique, 'have_fk':have, 'foreign_table':ft, 'foreign_key_name':fn, 'rank':rank})

        self.rm_scan.closeScan()
        return (False, None, None)
        
    def getAllAttrInfoFromCat(self, relName):
        table_list = self.getAllTableName()
        if relName not in table_list:
            raise myErr("OpenError", "The table doesn't exist.")
        self.rm_scan.openScan(self.attr_fhandle, ATTR_TYPE.STRING, MAX_NAME_LEN, 0, STRING_EQUAL_FUNC, relName)
        rid = Rid(1, -1)
        attrlist = []
        while True:
            record = self.rm_scan.getNextRecord(rid)
            rid = record.rid
            if rid == Rid(-1, -1):
                break

            attr_name = U_unpack_attr(record.data[MAX_NAME_LEN:2*MAX_NAME_LEN], ATTR_TYPE.STRING, MAX_NAME_LEN)
            ofs = U_unpack_attr(record.data[2*MAX_NAME_LEN:2*MAX_NAME_LEN+4], ATTR_TYPE.INT)
            aty = U_unpack_attr(record.data[2*MAX_NAME_LEN+4:2*MAX_NAME_LEN+8], ATTR_TYPE.INT)
            alh = U_unpack_attr(record.data[2*MAX_NAME_LEN+8:2*MAX_NAME_LEN+12], ATTR_TYPE.INT)
            ino = U_unpack_attr(record.data[2*MAX_NAME_LEN+12:2*MAX_NAME_LEN+16], ATTR_TYPE.INT)
            #null, default, default_is_null, unique, have_fk, foreign_table, foreign_key_name
            null = U_unpack_attr(record.data[2*MAX_NAME_LEN+16:2*MAX_NAME_LEN+20], ATTR_TYPE.INT)
            din = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+20:2*MAX_NAME_LEN+MAX_STRING_LEN+24], ATTR_TYPE.INT)
            unique = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+24:2*MAX_NAME_LEN+MAX_STRING_LEN+28], ATTR_TYPE.INT)
            have = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+28:2*MAX_NAME_LEN+MAX_STRING_LEN+32], ATTR_TYPE.INT)
            ft = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32:2*MAX_NAME_LEN+MAX_STRING_LEN+32+MAX_NAME_LEN], ATTR_TYPE.STRING, MAX_NAME_LEN).rstrip()
            fn = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32+MAX_NAME_LEN:2*MAX_NAME_LEN+MAX_STRING_LEN+32+3*MAX_NAME_LEN], ATTR_TYPE.STRING, 2*MAX_NAME_LEN).rstrip()
            rank = U_unpack_attr(record.data[2*MAX_NAME_LEN+MAX_STRING_LEN+32+3*MAX_NAME_LEN:2*MAX_NAME_LEN+MAX_STRING_LEN+36+3*MAX_NAME_LEN], ATTR_TYPE.INT)

            if aty == ATTR_TYPE.INT:
                default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+24], ATTR_TYPE.INT)
            elif aty == ATTR_TYPE.FLOAT:
                default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+24], ATTR_TYPE.FLOAT)
            else:
                default = U_unpack_attr(record.data[2*MAX_NAME_LEN+20:2*MAX_NAME_LEN+MAX_STRING_LEN+20], ATTR_TYPE.STRING, MAX_STRING_LEN).rstrip()
            info = {'relName':relName, 'attrName':attr_name, 'offset': ofs, 'attrType': aty, 'attrLength': alh, 'indexNo': ino,
            'null':null, 'default':default, 'default_is_null':din, 'unique':unique, 'have_fk':have, 'foreign_table':ft, 'foreign_key_name':fn, 'rank':rank}
            attrlist.append(info)

        self.rm_scan.closeScan()
        return attrlist


            
    @check_db_open
    def DropIndex(self, relName, attrName, droptable=False):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")


        exist, rid, info = self.getAttrInfoFromCat(relName, attrName)

        if not droptable:
            if not exist:
                raise myErr("AttrError", f"There isn't an attr named {attrName} on {relName}.")
            if info["indexNo"] == -1:
                raise myErr("IndexError", f"There isn't an index on {attrName} of {relName}.")
            if info["indexNo"] == 0:
                raise myErr("IndexError", "You can't delete the index of primary key.")
            if info["unique"] == 1:
                raise myErr("IndexError", "You can't delete the index of unique attr.")
        no = info["indexNo"]
        self.ixm.removeIndex(self.getDbTablePath(relName), no)
        self.rmm.removeFile(self.getDbTablePath(f"{relName}.index{no}null"))

        if not droptable:
            info["indexNo"] = -1
            record = RM_Record(U_pack_attrcat_data(info), rid)
            self.attr_fhandle.updateRecord(record)

    @check_db_open
    def CreateIndex(self, relName, attrName, simple=True, indexno=0):
        if attrName == "" or len(attrName.encode('utf-8')) > MAX_NAME_LEN:
            raise myErr("IllegalName", "The attr name is not allowed.")

        exit_attr, rid, attrInfo = self.getAttrInfoFromCat(relName, attrName)
        if not exit_attr:
            raise myErr("IllegalName", "The attr name is not in the attr pool.")
        if simple and attrInfo["indexNo"] != -1:
            raise myErr("Repeated", "The attr already has an index.")
        elif not simple and attrInfo["indexNo"] != -1:
            src_no = attrInfo["indexNo"]
            attrInfo["indexNo"] = indexno
            if indexno == 0:
                attrInfo["unique"] = 0 # 如果有unique约束，此时删去，因为主键约束更强
            record = RM_Record(U_pack_attrcat_data(attrInfo), rid)
            self.attr_fhandle.updateRecord(record)
            os.rename(self.getDbTablePath(f'{relName}.index{src_no}null'), self.getDbTablePath(f'{relName}.index{indexno}null'))
            os.rename(self.getDbTablePath(f'{relName}.{src_no}'), self.getDbTablePath(f'{relName}.{indexno}'))
            return


        if simple:
            indexno = attrInfo["offset"]
        #以offset来作为indexno，是唯一的
        self.ixm.createIndex(self.getDbTablePath(relName), indexno, attrInfo["attrType"], attrInfo["attrLength"])
        #创建一个null的记录
        self.rmm.createFile(self.getDbTablePath(f'{relName}.index{indexno}null'), 8)

        #更新attrcat表
        attrInfo['indexNo'] = indexno
        record = RM_Record(U_pack_attrcat_data(attrInfo), rid)
        self.attr_fhandle.updateRecord(record)

        ix_fh = self.ixm.openIndex(self.getDbTablePath(relName), attrInfo["indexNo"])
        rm_fh = self.rmm.openFile(self.getDbTablePath(relName))

        self.rm_scan.openScan(rm_fh, attrInfo["attrType"], attrInfo["attrLength"], attrInfo["offset"], STRING_EQUAL_FUNC, None) #这个func是随便传的，无所谓
        rid = Rid(1, -1)

        null_list = []

        # count = 0
        while True:
            record = self.rm_scan.getNextRecord(rid)
            rid = record.rid
            if rid == Rid(-1, -1):
                break
            if U_check_if_attr_is_null(record.data, attrInfo["rank"]):
                null_list.append(rid)
                continue
            # count += 1
            # if count % 10000 == 0:
            #     print(f"进度{count}.")
            raw_data_ = U_unpack_attr(record.data[attrInfo["offset"]:attrInfo["offset"]+attrInfo["attrLength"]], attrInfo["attrType"], attrInfo["attrLength"])
            ix_fh.insertEntry(raw_data_, rid)

        self.rm_scan.closeScan()
        self.ixm.closeIndex(ix_fh)
        self.rmm.closeFile(rm_fh)

        if null_list:
            rm_fh = self.rmm.openFile(self.getDbTablePath(f'{relName}.index{indexno}null'))
            for null_rid in null_list:
                da = U_pack_attr_array([null_rid.page_num, null_rid.slot_num], ATTR_TYPE.INT, 4, 2)
                rm_fh.insertRecord(da)

            self.rmm.closeFile(rm_fh)

    @check_db_open
    def Delete(self, relName, rid_list):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
        if len(rid_list) == 0:
            return 0

        attrlist = self.getAllAttrInfoFromCat(relName)
        all_foreign_vice_info = []
        primary_info = None
        primary_value_list = []
        index_infos = []


        for attrl in attrlist:
            if attrl["indexNo"] != -1:
                index_infos.append(attrl)
            if attrl["indexNo"] == 0:
                #主键，得查看所有的外键
                tables = self.getAllTableName()
                primary_info = attrl
                for table in tables:
                    infos_ = self.getAllAttrInfoFromCat(table)
                    for info_ in infos_:
                        if info_["have_fk"] == 1:
                            main_table = info_["foreign_table"]
                            if main_table == relName:
                                all_foreign_vice_info.append(info_)

        index_value_list_list = [[]] * len(index_infos)

        if all_foreign_vice_info or index_infos:
            rmfh = self.rmm.openFile(self.getDbTablePath(relName))
            for rid in rid_list:
                record = rmfh.getRecord(rid)
                if all_foreign_vice_info:
                    offset = primary_info["offset"]
                    typ = primary_info["attrType"]
                    length = primary_info["attrLength"]
                    raw_data_ = U_unpack_attr(record.data[offset:offset+length], typ, length)
                    primary_value_list.append(raw_data_)
                if index_infos:
                    for i, index_info in enumerate(index_infos):
                        offset = index_info["offset"]
                        typ = index_info["attrType"]
                        length = index_info["attrLength"]
                        rank = index_info["rank"]
                        is_null = U_check_if_attr_is_null(record.data, rank)
                        if not is_null:
                            raw_data_ = U_unpack_attr(record.data[offset:offset+length], typ, length)
                        else:
                            raw_data_ = 0
                        index_value_list_list[i].append((is_null, raw_data_))
            self.rmm.closeFile(rmfh)

        if all_foreign_vice_info:
            all_find = False
            for foreign_vice_info in all_foreign_vice_info:
                if foreign_vice_info["indexNo"] != -1:
                    ixfh = self.ixm.openIndex(self.getDbTablePath(foreign_vice_info["relName"]), foreign_vice_info["indexNo"])
                    for v in primary_value_list:
                        self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, v)
                        find, value, rid = self.ix_scan.getNextEntry()
                        self.ix_scan.closeScan()
                        if find:
                            all_find = True
                            break
                    self.ixm.closeIndex(ixfh)
                else:
                    offset = foreign_vice_info["offset"]
                    length = foreign_vice_info["attrLength"]
                    typ = foreign_vice_info["attrType"]
                    rmfh = self.rmm.openFile(self.getDbTablePath(foreign_vice_info["relName"]))
                    self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                    rid = Rid(1, -1)
                    while True:
                        record = self.rm_scan.getNextRecord(rid)
                        rid = record.rid
                        if rid == Rid(-1, -1):
                            break
                        data = record.data
                        attr = U_unpack_attr(data[offset:offset+length], typ, length)
                        if attr in primary_value_list:
                            all_find = True
                            break
                    self.rm_scan.closeScan()
                    self.rmm.closeFile(rmfh)
                if all_find:
                    break
            if all_find:
                raise myErr("DeleteError", f"You violate the foreign key constraint on the primary key {primary_info['attrName']}.")


        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        for rid in rid_list:
            rmfh.deleteRecord(rid)
        self.rmm.closeFile(rmfh)
        
        for i, index_info in enumerate(index_infos):
            indexno = index_info["indexNo"]
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), indexno)
            rmfh = self.rmm.openFile(self.getDbTablePath(f"{relName}.index{indexno}null"))

            index_value_list = index_value_list_list[i]
            null_rid_list = []
            for j, l in enumerate(index_value_list):
                is_null, value = l
                if is_null:
                    null_rid_list.append((rid_list[j].page_num, rid_list[j].slot_num))
                else:
                    ixfh.deleteEntry(value, rid_list[j])
            if null_rid_list:
                real_rid_list = []
                self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                rid = Rid(1, -1)
                while True:
                    record = self.rm_scan.getNextRecord(rid)
                    rid = record.rid
                    if rid == Rid(-1, -1):
                        break
                    data = record.data
                    page_num = U_unpack_attr(data[0:4], ATTR_TYPE.INT, 4)
                    slot_num = U_unpack_attr(data[4:8], ATTR_TYPE.INT, 4)
                    if (page_num, slot_num) in null_rid_list:
                        real_rid_list.append(rid)
                    if len(real_rid_list) == len(null_rid_list):
                        break
                self.rm_scan.closeScan()
                for real_rid in real_rid_list:
                    rmfh.deleteRecord(real_rid)
            self.rmm.closeFile(rmfh)
            self.ixm.closeIndex(ixfh)
        return len(rid_list)

    @check_db_open
    def Update(self, relName, set_clause_list, rid_list):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("OpenError", "The table doesn't exist.")
        if len(rid_list) == 0:
            return
        attrlist = self.getAllAttrInfoFromCat(relName)
        update_list = []
        index_infos = []
        index_values = []
        #先检查type和name：
        for set_clause in set_clause_list:
            column = set_clause[0]
            value, typ = set_clause[1]
            find = False
            for attrl in attrlist:
                if attrl["attrName"] == column:
                    if typ != ATTR_TYPE.NONETYPE and typ != attrl["attrType"] \
                    and not (typ == ATTR_TYPE.INT and attrl["attrType"] == ATTR_TYPE.FLOAT):
                        raise myErr("UpdateError", "The type of update value is wrong.")
                    update_list.append((attrl, value, typ))
                    if attrl["indexNo"] != -1:
                        index_infos.append(attrl)
                        index_values.append((attrl, value, typ))
                    find = True
                    break
            if not find:
                raise myErr("UpdateError", f"The column name {column} doesn't exist.")

        pk_info = None
        pk_value = None
        fk_value_dict = {}
        unique_value_dict = {}

        index_value_list_dict = {}

        #考虑update的值符不符合insert约束
        for attrl, value, type_gave in update_list:
            typ = attrl["attrType"]
            length = attrl["attrLength"]
            indexno = attrl["indexNo"]
            rank = attrl["rank"]
            have_fk = attrl["have_fk"]
            attrname = attrl["attrName"]
            if type_gave == ATTR_TYPE.NONETYPE:
                if attrl["null"] == 0:
                    raise myErr("UpdateError", f"At the {attrname} column: you can't insert null into NOT NULL column.")
                if indexno == 0:
                    raise myErr("UpdateError", f"At the {attrname} column: you can't insert null into primary key.")
                if have_fk == 1:
                    raise myErr("UpdateError", f"At the {attrname} column: you can't insert null into the column which has foreign key constraint.")
            else:
                if indexno == 0:
                    pk_info = attrl
                    pk_value = value
                    if len(rid_list) > 1:
                        raise myErr("UpdateError", "Your updating makes primary key has two or more repeated values.")
                if attrl["unique"] == 1:
                    unique_value_dict[indexno] = value
                    if len(rid_list) > 1:
                        raise myErr("UpdateError", f"Your updating makes unique column {attrname} has two or more repeated values.")
                if have_fk == 1:
                    main_table = attrl["foreign_table"]
                    if main_table in fk_value_dict:
                        fk_value_dict[main_table].append(value)
                    else:
                        fk_value_dict[main_table] = [value]

        for key in unique_value_dict:
            #能到这里的，rid_list都只有一个
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), key)
            value = unique_value_dict[key]
            self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, value)
            repeated, _, rid = self.ix_scan.getNextEntry()
            self.ix_scan.closeScan()
            self.ixm.closeIndex(ixfh)
            if repeated and not (rid_list[0] == rid):
                #set的值已经存在，且不是要改的rid的值
                raise myErr("UpdateError", "There is unique constraint and your updating destory it.")

        for key in fk_value_dict:
            if self.check_if_pk_is_repeated(key, fk_value_dict[key], False):
                raise myErr("UpateError", f"The value you insert doesn't exist in reference table {key}'s primary key.")

        if pk_info is not None:
            #能到这里的，rid_list都只有一个
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), 0)
            self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, pk_value)
            repeated, delete_value, rid = self.ix_scan.getNextEntry()
            self.ix_scan.closeScan()
            self.ixm.closeIndex(ixfh)
            if repeated and not (rid_list[0] == rid):
                #set的值已经存在，且不是要改的rid的值
                raise myErr("UpdateError", "There is primary key constraint and your updating destory it.")
            if repeated:
                #说明改了和没改一样，那不用检查delete约束了
                #接着检查delete的约束
                
                all_foreign_vice_info = []
                tables = self.getAllTableName()
                for table in tables:
                    infos_ = self.getAllAttrInfoFromCat(table)
                    for info_ in infos_:
                        if info_["have_fk"] == 1:
                            main_table = info_["foreign_table"]
                            if main_table == relName:
                                all_foreign_vice_info.append(info_)

                if all_foreign_vice_info:
                    all_find = False
                    for foreign_vice_info in all_foreign_vice_info:
                        if foreign_vice_info["indexNo"] != -1:
                            ixfh = self.ixm.openIndex(self.getDbTablePath(foreign_vice_info["relName"]), foreign_vice_info["indexNo"])
                            self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, delete_value)
                            find, _, rid = self.ix_scan.getNextEntry()
                            self.ix_scan.closeScan()
                            self.ixm.closeIndex(ixfh)
                            if find:
                                all_find = True
                                break
                        else:
                            offset = foreign_vice_info["offset"]
                            length = foreign_vice_info["attrLength"]
                            typ = foreign_vice_info["attrType"]
                            rmfh = self.rmm.openFile(self.getDbTablePath(foreign_vice_info["relName"]))
                            self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                            rid = Rid(1, -1)
                            while True:
                                record = self.rm_scan.getNextRecord(rid)
                                rid = record.rid
                                if rid == Rid(-1, -1):
                                    break
                                data = record.data
                                attr = U_unpack_attr(data[offset:offset+length], typ, length)
                                if attr == delete_value:
                                    all_find = True
                                    break
                            self.rm_scan.closeScan()
                            self.rmm.closeFile(rmfh)
                        if all_find:
                            break
                    if all_find:
                        raise myErr("UpdateError", f"You violate the foreign key constraint on the primary key {pk_info['attrName']}.")

        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        for rid in rid_list:
            record = rmfh.getRecord(rid)
            for attrl, value, type_gave in update_list:
                offset = attrl["offset"]
                typ = attrl["attrType"]
                length = attrl["attrLength"]
                indexno = attrl["indexNo"]
                rank = attrl["rank"]
                have_fk = attrl["have_fk"]
                attrname = attrl["attrName"]
                is_null = U_check_if_attr_is_null(record.data, rank)
                if indexno != -1:
                    if not is_null:
                        raw_data_ = U_unpack_attr(record.data[offset:offset+length], typ, length)
                    else:
                        raw_data_ = 0
                    if indexno in index_value_list_dict:
                        index_value_list_dict[indexno].append((is_null, raw_data_))
                    else:
                        index_value_list_dict[indexno] = [(is_null, raw_data_)]
                
                if type_gave == ATTR_TYPE.NONETYPE:
                    if not is_null:
                        record.data[rank // 8] += 1 << (7 - rank % 8)
                else:
                    if is_null:
                        record.data[rank // 8] -= 1 << (7 - rank % 8)
                    arr = U_pack_attr(value, typ, length)
                    record.data[offset:offset+length] = arr.copy()
            rmfh.updateRecord(record)
        self.rmm.closeFile(rmfh)

        #索引增删
        for i, index_info in enumerate(index_infos):
            indexno = index_info["indexNo"]
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), indexno)
            rmfh = self.rmm.openFile(self.getDbTablePath(f"{relName}.index{indexno}null"))

            index_value_list = index_value_list_dict[indexno]
            null_rid_list = []
            for j, l in enumerate(index_value_list):
                is_null, value = l
                if is_null:
                    null_rid_list.append((rid_list[j].page_num, rid_list[j].slot_num))
                else:
                    ixfh.deleteEntry(value, rid_list[j])
            if null_rid_list:
                real_rid_list = []
                self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                rid = Rid(1, -1)
                while True:
                    record = self.rm_scan.getNextRecord(rid)
                    rid = record.rid
                    if rid == Rid(-1, -1):
                        break
                    data = record.data
                    page_num = U_unpack_attr(data[0:4], ATTR_TYPE.INT, 4)
                    slot_num = U_unpack_attr(data[4:8], ATTR_TYPE.INT, 4)
                    if (page_num, slot_num) in null_rid_list:
                        real_rid_list.append(rid)
                    if len(real_rid_list) == len(null_rid_list):
                        break
                self.rm_scan.closeScan()
                for real_rid in real_rid_list:
                    rmfh.deleteRecord(real_rid)
            
            _, value_, typ_ = index_values[i]
            for rid_ in rid_list:
                arr_ = U_pack_attr_array([rid_.page_num, rid_.slot_num], ATTR_TYPE.INT, 4, 2)
                if typ_ == ATTR_TYPE.NONETYPE:
                    rmfh.insertRecord(arr_)
                else:
                    ixfh.insertEntry(value_, rid_)

            self.rmm.closeFile(rmfh)
            self.ixm.closeIndex(ixfh)

    @check_db_open
    def LoadData(self, relName, value_list):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("OpenError", "The table doesn't exist.")
        attrlist = self.getAllAttrInfoFromCat(relName)
        attr_type_list = []
        for i in range(len(attrlist)):
            for attr in attrlist:
                if i == attr["rank"]:
                    attr_type_list.append(attr["attrType"])
                    break
        values = []
        for i, v in enumerate(value_list):
            if i % 10000 == 0:
                values.append([])
            atl = [k for k in attr_type_list]
            for j, k in enumerate(attr_type_list):
                if v[j] == "NONE":
                    v[j] = "NULL"
                    atl[j] = ATTR_TYPE.NONETYPE
                if k == ATTR_TYPE.INT:
                    v[j] = int(v[j])
                elif k == ATTR_TYPE.FLOAT:
                    v[j] = float(v[j])
            values[-1].append([v, atl])

        nums = len(value_list)
        print(f"共{nums}条记录。开始插入：")
        for i, vs in enumerate(values):
            self.Insert_RoutineWork(relName, vs)
            print(f"插入了{i*10000 + len(vs)}条，进度{((i*10000+len(vs))/nums)*100}%。")


    @check_db_open
    def Insert_RoutineWork(self, relName, values):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("OpenError", "The table doesn't exist.")
        attrlist = self.getAllAttrInfoFromCat(relName)
        attr_num = len(attrlist)
        if len(attrlist) == 0:
            raise myErr("WrongValueList", "You gave a wrong value list.")
        pk_value_list = []
        fk_value_dict = {}
        unique_value_dict = {}
        arr_list_list = []
        for idx, l_ in enumerate(values):
            value_list, type_list = l_

            if len(attrlist) != len(value_list):
                raise myErr("WrongValueList", f"At the {idx+1}th VALUE: you gave a wrong value list.")
            null_tag = np.array([0] * ((attr_num-1)//8+1)).astype(np.uint8)
            arr_list = [0] * attr_num
            for attrl in attrlist:
                typ = attrl["attrType"]
                length = attrl["attrLength"]
                indexno = attrl["indexNo"]
                rank = attrl["rank"]
                value = value_list[rank]
                type_gave = type_list[rank]
                have_fk = attrl["have_fk"]
                if type_gave == ATTR_TYPE.NONETYPE:
                    if attrl["null"] == 0:
                        raise myErr("InsertError", f"At the {idx+1}th VALUE: you can't insert null into NOT NULL column.")
                    if indexno == 0:
                        raise myErr("InsertError", f"At the {idx+1}th VALUE: you can't insert null into primary key.")
                    if have_fk == 1:
                        raise myErr("InsertError", f"At the {idx+1}th VALUE: you can't insert null into the column which has foreign key constraint.")
                    null_tag[rank // 8] += 1 << (7 - rank % 8)
                    v = 0 if typ != ATTR_TYPE.STRING else " "
                    arr = U_pack_attr(v, typ, length)
                elif type_gave == typ or (type_gave == ATTR_TYPE.INT and typ == ATTR_TYPE.FLOAT):
                    if indexno == 0:
                        pk_value_list.append(value)
                    if attrl["unique"] == 1:
                        if indexno in unique_value_dict:
                            unique_value_dict[indexno].append(value)
                        else:
                            unique_value_dict[indexno] = [value]
                    if have_fk == 1:
                        main_table = attrl["foreign_table"]
                        if main_table in fk_value_dict:
                            fk_value_dict[main_table].append(value)
                        else:
                            fk_value_dict[main_table] = [value]
                    arr = U_pack_attr(value, typ, length)
                else:
                    raise myErr("InsertError", "The value type is not consistent with the column.")
                arr_list[rank] = arr
            for arr in arr_list:
                null_tag = np.concatenate((null_tag, arr))
            arr_list_list.append(null_tag)

        if pk_value_list and self.check_if_pk_is_repeated(relName, pk_value_list, True):
            raise myErr("InsertError", "You insert a value existing into the primary key.")
        for key in fk_value_dict:
            if self.check_if_pk_is_repeated(key, fk_value_dict[key], False):
                raise myErr("InsertError", f"The value you insert doesn't exist in reference table {key}'s primary key.")
        for key in unique_value_dict:
            if self.check_if_unique_is_repeated(relName, key, unique_value_dict[key]):
                raise myErr("InsertError", "The column has unique constraint and you insert a value existing.")

        rid_list = []
        data_list = []
        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        for arr_ in arr_list_list:
            rid_ = rmfh.insertRecord(arr_)
            rid_list.append(rid_)
            data_list.append(U_pack_attr_array([rid_.page_num, rid_.slot_num], ATTR_TYPE.INT, 4, 2))
        self.rmm.closeFile(rmfh)

        for attr in attrlist:
            if attr["indexNo"] != -1:
                rmfh = self.rmm.openFile(self.getDbTablePath(f"{relName}.index{attr['indexNo']}null"))
                ixfh = self.ixm.openIndex(self.getDbTablePath(relName), attr['indexNo'])
                for idx, l_ in enumerate(values):
                    value_list, type_list = l_
                    v = value_list[attr["rank"]]
                    t = type_list[attr["rank"]]
                    data = data_list[idx]
                    rid = rid_list[idx]
                    if t == ATTR_TYPE.NONETYPE:
                        rmfh.insertRecord(data)
                    else:
                        ixfh.insertEntry(v, rid)
                self.rmm.closeFile(rmfh)
                self.ixm.closeIndex(ixfh)




    # @check_db_open
    # def Insert(self, relName, value_list, type_list):
    #     if not os.path.exists(self.getDbTablePath(relName)):
    #         raise myErr("OpenError", "The table doesn't exist.")
    #     attrlist = self.getAllAttrInfoFromCat(relName)
    #     if len(attrlist) == 0 or len(attrlist) != len(value_list):
    #         raise myErr("WrongValueList", "You gave a wrong value list.")

    #     attr_num = len(attrlist)
    #     null_tag = np.array([0] * (attr_num//8+1)).astype(np.uint8)
    #     arr_list = []
    #     for i, attrl in enumerate(attrlist):
    #         typ = attrl["attrType"]
    #         length = attrl["attrLength"]
    #         indexno = attrl["indexNo"]
    #         rank = attrl["rank"]
    #         value = value_list[rank]
    #         have_fk = attrl["have_fk"]
    #         if value == "NULL":
    #             if attrl["null"] == 0:
    #                 raise myErr("InsertError", "You can't insert null into NOT NULL column.")
    #             if indexno == 0:
    #                 raise myErr("InsertError", "You can't insert null into primary key.")
    #             if have_fk == 1:
    #                 raise myErr("InsertError", "You can't insert null into the column which has foreign key constraint.")
    #             null_tag[i // 8] += 1 << (7 - i % 8)
    #             v = 0 if typ != ATTR_TYPE.STRING else " "
    #             arr = U_pack_attr(v, typ, length)
    #         elif type_list[i] == typ or (type_list[i] == ATTR_TYPE.INT and typ == ATTR_TYPE.FLOAT):
    #             arr = U_pack_attr(value, typ, length)
    #             if indexno == 0 and self.check_if_pk_is_repeated(relName, value):
    #                 raise myErr("InsertError", "The key is primary key and you insert a value existing.")
    #             if attrl["unique"] == 1 and self.check_if_unique_is_repeated(relName, attrl, value):
    #                 raise myErr("InsertError", "The column has unique constraint and you insert a value existing.")
    #             if have_fk == 1:
    #                 main_table = attrl["foreign_table"]
    #                 if not self.check_if_pk_is_repeated(main_table, value):
    #                     raise myErr("InsertError", f"The column {attrl['attrName']} has foreign key and the value you insert doesn't exist in reference column.")
    #         else:
    #             raise myErr("InsertError", "The value type is not consistent with the column.")
    #         arr_list.append(arr)


    #     for arr in arr_list:
    #         null_tag = np.concatenate((null_tag, arr))
    #     rmfh = self.rmm.openFile(self.getDbTablePath(relName))

    #     rid_ = rmfh.insertRecord(null_tag)
    #     self.rmm.closeFile(rmfh)

    #     #向所有需要索引的增加东西：
    #     data = U_pack_attr_array([rid_.page_num, rid_.slot_num], ATTR_TYPE.INT, 4, 2)
    #     for attr in attrlist:
    #         if attr["indexNo"] != -1:
    #             v = value_list[attr["rank"]]
    #             if v == "NULL":
    #                 rmfh = self.rmm.openFile(self.getDbTablePath(f"{relName}.index{attr['indexNo']}null"))
    #                 rmfh.insertRecord(data)
    #                 self.rmm.closeFile(rmfh)
    #             else:
    #                 ixfh = self.ixm.openIndex(self.getDbTablePath(relName), attr['indexNo'])
    #                 ixfh.insertEntry(v, rid_)
    #                 self.ixm.closeIndex(ixfh)

    @check_db_open
    def Select_with_WhereClause_Join(self, relName_List, attrList_list, rid_list_a, rid_list_b, is_true:bool, cross_clause_list: list, limit=None, offset=0):
        for relName in relName_List:
            if not os.path.exists(self.getDbTablePath(relName)):
                raise myErr("TableError", f"There isn't a table named {relName}.")
        if len(relName_List) == 1:
            if limit is not None:
                rid_list_a = rid_list_a[offset:offset+limit]
            if not attrList_list:
                return self.Select_with_WhereClause(relName_List[0], [], rid_list_a)
            attrlist = [attr[1] for attr in attrList_list]
            return self.Select_with_WhereClause(relName_List[0], attrlist, rid_list_a)
        else:
            A_attrlist = []
            B_attrlist = []
            #为了cross_clause_list新增的东西qaq
            A_cross_list = [clause[1] for clause in cross_clause_list]
            B_cross_list = [clause[2] for clause in cross_clause_list]
            op_list = [clause[0] for clause in cross_clause_list]
            A_cross_attrlist = []
            B_cross_attrlist = []
            #先检查cross_clause_list里的内容
            if cross_clause_list:
                for relName in relName_List:
                    if not os.path.exists(self.getDbTablePath(relName)):
                        raise myErr("TableError", f"There isn't a table named {relName}.")
                A_attr_list = self.getAllAttrInfoFromCat(relName_List[0])
                B_attr_list = self.getAllAttrInfoFromCat(relName_List[1])
                for i, op in enumerate(op_list):
                    for attr in A_attr_list:
                        if attr["attrName"] == A_cross_list[i]:
                            A_cross_attrlist.append(attr)
                            break
                    for attr in B_attr_list:
                        if attr["attrName"] == B_cross_list[i]:
                            B_cross_attrlist.append(attr)
                            break
                    if len(A_cross_attrlist) != i + 1 or len(B_cross_attrlist) != i + 1:
                        raise myErr("AttrError", "You gave a wrong attr name.")
                    at = A_cross_attrlist[-1]["attrType"]
                    bt = B_cross_attrlist[-1]["attrType"]
                    if at + bt != 3 and at != bt:
                        raise myErr("TypeError", "The two columns with different types can't be compared.")


            if attrList_list:
                for attr in attrList_list:
                    if attr[0] == relName_List[0]:
                        A_attrlist.append(attr[1])
                    elif attr[0] == relName_List[1]:
                        B_attrlist.append(attr[1])
                    else:
                        raise myErr("TableError", "You gave a wrong table name.")

            if not is_true and len(rid_list_b) == 0:
                namelist_b, result_b = self.Select(relName_List[1], B_attrlist, B_cross_attrlist)
            else:
                namelist_b, result_b = self.Select_with_WhereClause(relName_List[1], B_attrlist, rid_list_b, B_cross_attrlist)
            if not is_true and len(rid_list_a) == 0:
                namelist_a, result_a = self.Select(relName_List[0], A_attrlist, A_cross_attrlist)
            else:
                namelist_a, result_a = self.Select_with_WhereClause(relName_List[0], A_attrlist, rid_list_a, A_cross_attrlist)
            if cross_clause_list:
                a_cross_ = [result[-len(op_list):] for result in result_a]
                b_cross_ = [result[-len(op_list):] for result in result_b]
                result_a = [result[:-len(op_list)] for result in result_a]
                result_b = [result[:-len(op_list)] for result in result_b]
                end_result = self.getCrossJoin(result_a, result_b, len(A_attrlist)!=0 or len(attrList_list)==0, len(B_attrlist)!=0 or len(attrList_list)==0, True, op_list, a_cross_, b_cross_)
            else:
                end_result = self.getCrossJoin(result_a, result_b, len(A_attrlist)!=0 or len(attrList_list)==0, len(B_attrlist)!=0 or len(attrList_list)==0, False)
            
            if cross_clause_list:
                namelist_a = namelist_a[:-len(op_list)]
                namelist_b = namelist_b[:-len(op_list)]
            if len(attrList_list) == 0 or (A_attrlist and B_attrlist):
                end_namelist = self.getCrossJoin_Namelist(namelist_a, namelist_b, relName_List[0], relName_List[1])
            elif not A_attrlist:
                end_namelist = self.getCrossJoin_Namelist([], namelist_b, relName_List[0], relName_List[1])
            else:
                end_namelist = self.getCrossJoin_Namelist(namelist_a, [], relName_List[0], relName_List[1])
            if limit is not None:
                end_result = end_result[offset:offset+limit]   
            return end_namelist, end_result

    @check_db_open
    def Select_with_WhereClause(self, relName, attrList, rid_list, cross_list=None):
        if cross_list is None:
            cross_list = []
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")

        attr_list = self.getAllAttrInfoFromCat(relName)

        if attrList:
            real_attrlist = []
            for attr in attrList:
                for attr_ in attr_list:
                    if attr == attr_["attrName"]:
                        real_attrlist.append(attr_)
                        break
            if len(real_attrlist) != len(attrList):
                raise myErr("AttrError", "You gave (a) wrong attr(s).")
        else:
            real_attrlist = attr_list
        real_attrlist.extend(cross_list)
        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        result = []
        for rid in rid_list:
            record = rmfh.getRecord(rid)
            record_data = record.data
            l = []
            for attr in real_attrlist:
                if U_check_if_attr_is_null(record_data, attr["rank"]):
                    l.append("NULL")
                    continue
                typ = attr["attrType"]
                length = attr["attrLength"]
                offset = attr["offset"]
                l.append(U_unpack_attr(record_data[offset:offset+length], typ, length))
            result.append(l)
        self.rmm.closeFile(rmfh)
        rel_name_list = [attr["attrName"] for attr in real_attrlist]
        return rel_name_list, result

    def getCrossJoin(self, la, lb, la_is_valid:bool, lb_is_valid:bool, condition=False, op_list=None, a_cross_list=None, b_cross_list=None):
        #la和acrosslist的元素长度应该是相等的
        if condition:
            result = []
            for i, lla in enumerate(la):
                for j, llb in enumerate(lb):
                    for k, op in enumerate(op_list):
                        a_val = a_cross_list[i][k]
                        b_val = b_cross_list[j][k]
                        if U_COMP_OP_to_Bool(op, a_val, b_val):
                            if not la_is_valid:
                                result.append(llb)
                            elif not lb_is_valid:
                                result.append(lla)
                            else:
                                result.append(lla+llb)
            return result
        else:
            if not la_is_valid:
                return lb * (len(la))
            elif not lb_is_valid:
                return la * (len(lb))
            else:
                return [lla+llb for lla in la for llb in lb]

    def getCrossJoin_Namelist(self, la, lb, relName_a, relName_b):
        namelist = []
        for lla in la:
            namelist.append(f"{relName_a}.{lla}")
        for llb in lb:
            namelist.append(f"{relName_b}.{llb}")
        return namelist

    @check_db_open
    def Select_Join(self, relName_List, attrList_list, limit=None, offset=0):
        # sourcery skip: remove-unnecessary-else, swap-if-else-branches
        #attrList_list的格式是[[relName, attrName], [relName, attrName], []...] 或 []
        for relName in relName_List:
            if not os.path.exists(self.getDbTablePath(relName)):
                raise myErr("TableError", f"There isn't a table named {relName}.")
        if len(relName_List) == 1:
            if attrList_list:
                attrlist = [attr[1] for attr in attrList_list]
                return self.Select(relName_List[0], attrlist, None, limit, offset)
            else:
                return self.Select(relName_List[0], [], None, limit, offset)
        else:
            A_attrlist = []
            B_attrlist = []
            if attrList_list:
                for attr in attrList_list:
                    if attr[0] == relName_List[0]:
                        A_attrlist.append(attr[1])
                    elif attr[0] == relName_List[1]:
                        B_attrlist.append(attr[1])
                    else:
                        raise myErr("TableError", "You gave a wrong table name.")
            namelist_b, result_b = self.Select(relName_List[1], B_attrlist)
            namelist_a, result_a = self.Select(relName_List[0], A_attrlist)
            end_result = self.getCrossJoin(result_a, result_b, len(A_attrlist)!=0 or len(attrList_list)==0, len(B_attrlist)!=0 or len(attrList_list)==0)
            if len(attrList_list) == 0 or (A_attrlist and B_attrlist):
                end_namelist = self.getCrossJoin_Namelist(namelist_a, namelist_b, relName_List[0], relName_List[1])
            elif not A_attrlist:
                end_namelist = self.getCrossJoin_Namelist([], namelist_b, relName_List[0], relName_List[1])
            else:
                end_namelist = self.getCrossJoin_Namelist(namelist_a, [], relName_List[0], relName_List[1])
            if limit is not None:
                end_result = end_result[offset:offset+limit]
            return end_namelist, end_result


    @check_db_open
    def Select(self, relName, attrList, cross_list=None, limit=None, offset=0):
        if cross_list is None:
            cross_list = []
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")

        attr_list = self.getAllAttrInfoFromCat(relName)

        if attrList:
            real_attrlist = []
            for attr in attrList:
                for attr_ in attr_list:
                    if attr == attr_["attrName"]:
                        real_attrlist.append(attr_)
                        break
            if len(real_attrlist) != len(attrList):
                raise myErr("AttrError", "You gave (a) wrong attr(s).")
        else:
            real_attrlist = attr_list

        real_attrlist.extend(cross_list)
        rmfh = self.rmm.openFile(self.getDbTablePath(relName))

        self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)

        rid = Rid(1, -1)
        record_list = []
        now_count = 0
        while True:
            record = self.rm_scan.getNextRecord(rid)
            rid = record.rid
            if rid == Rid(-1, -1):
                break
            if limit is not None:
                now_count += 1
                if now_count <= offset:
                    continue
                if now_count > offset + limit:
                    break

            record_list.append(record.data)


        result = []
        for record_data in record_list:
            l = []
            for attr in real_attrlist:
                if U_check_if_attr_is_null(record_data, attr["rank"]):
                    l.append("NULL")
                    continue
                typ = attr["attrType"]
                length = attr["attrLength"]
                offset = attr["offset"]
                l.append(U_unpack_attr(record_data[offset:offset+length], typ, length))
            result.append(l)

        rel_name_list = [attr["attrName"] for attr in real_attrlist]

        self.rm_scan.closeScan()
        self.rmm.closeFile(rmfh)
        return rel_name_list, result


    def close(self):
        if self.attr_fhandle is not None and self.attr_fhandle.is_file_open:
            self.rmm.closeFile(self.attr_fhandle)

    @check_db_open
    def Describe(self, relName):
        attrlist = self.getAllAttrInfoFromCat(relName)
        result = []
        pk = None
        unique = []
        index = []
        fk_list = []
        for attr in attrlist:
            field = attr["attrName"]
            typ_ = attr["attrType"]
            if typ_ == ATTR_TYPE.INT:
                typ = "INT"
            elif typ_ == ATTR_TYPE.FLOAT:
                typ = "FLOAT"
            else:
                length = attr["attrLength"]
                typ = f"VARCHAR({length})"
            null = "YES" if attr["null"] == 1 else "NO"
            default = "NULL" if attr["default_is_null"] == 1 else attr["default"]
            result.append([field, typ, null, default])
            if attr["indexNo"] == 0:
                pk = field
            if attr["unique"] == 1:
                unique.append(field)
            elif attr["indexNo"] > 0:
                index.append(field)
            if attr["have_fk"] == 1:
                #fk_name, vice_column, main_table, main_column
                fkn = attr["foreign_key_name"]
                idx = fkn.find(".")
                fk_list.append([fkn[:idx], field, attr["foreign_table"], fkn[idx+1:]])
        return result, pk, unique, index, fk_list
    
    @check_db_open
    def AddPK(self, relName, pkName, check=True):
        #不能重复，不能空
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")

        attrInfos = self.getAllAttrInfoFromCat(relName)
        pkinfo = None
        for a in attrInfos:
            if a["indexNo"] == 0:
                raise myErr("AddPKError", "There already exists a primary key on this table.")
            if a["attrName"] == pkName:
                pkinfo = a
        if pkinfo is None:
            raise myErr("AttrError", "You gave a wrong attr.")

        if check:
            offset = pkinfo["offset"]
            length = pkinfo["attrLength"]
            typ = pkinfo["attrType"]
            rank = pkinfo["rank"]
            rmfh = self.rmm.openFile(self.getDbTablePath(relName))
            self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
            rid = Rid(1, -1)
            count = 0
            attr_set = set()
            raiseerr = False
            while True:
                record = self.rm_scan.getNextRecord(rid)
                rid = record.rid
                if rid == Rid(-1, -1):
                    break
                count += 1
                data = record.data
                if U_check_if_attr_is_null(data, rank):
                    self.rm_scan.closeScan()
                    self.rmm.closeFile(rmfh)
                    raise myErr("AddPKError", "The column has NULL value.")
                attr = U_unpack_attr(data[offset:offset+length], typ, length)
                if attr in attr_set:
                    raiseerr = True
                    break
                attr_set.add(attr)
            self.rm_scan.closeScan()
            self.rmm.closeFile(rmfh)
            if raiseerr:
                raise myErr("AddPKError", "The column has repeated value.")
        self.CreateIndex(relName, pkName, False, 0)

    @check_db_open
    def DropPK(self, relName):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
        attrInfos = self.getAllAttrInfoFromCat(relName)
        pkinfo = None
        for a in attrInfos:
            if a["indexNo"] == 0:
                pkinfo = a
        if pkinfo is None:
            raise myErr("DropPKError", "There isn't a primary key on this table.")

        #检测是否有外键，如果有不能删
        alltable = self.getAllTableName()
        for table in alltable:
            infos_ = self.getAllAttrInfoFromCat(table)
            for info in infos_:
                if info["have_fk"] == 1 and info["foreign_table"] == relName:
                    raise myErr("DropPKError", f"The {table}.{info['attrName']} has foreign key constraint and is referred to table {relName}.")

        self.ixm.removeIndex(self.getDbTablePath(relName), 0)
        self.rmm.removeFile(self.getDbTablePath(f"{relName}.index0null"))

        _, rid, info = self.getAttrInfoFromCat(relName, pkinfo["attrName"])
        info["indexNo"] = -1
        record = RM_Record(U_pack_attrcat_data(info), rid)
        self.attr_fhandle.updateRecord(record)

    @check_db_open
    def AddUnique(self, relName, attrName):
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
        exist, rid_, info = self.getAttrInfoFromCat(relName, attrName)
        if not exist:
            raise myErr("AttrError", f"There isn't an attr named {attrName} on {relName}.")
        if info["indexNo"] == 0:
            raise myErr("AddUniqueError", "The column is already a primary key.")
        if info["unique"] == 1:
            raise myErr("AddUniqueError", "The column already has the unique constraint.")
        #先判断现在是否符合unique要求:
        there_is_index = False
        pool = set()
        count = 0
        raiseerr = False
        if info["indexNo"] != -1:
            there_is_index = True
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), info["indexNo"])
            self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
            while True:
                find, value, rid = self.ix_scan.getNextEntry()
                if not find:
                    break
                if value in pool:
                    raiseerr = True
                    break
                pool.add(value)
                count += 1
            self.ix_scan.closeScan()
            self.ixm.closeIndex(ixfh)
        else:
            offset = info["offset"]
            length = info["attrLength"]
            typ = info["attrType"]
            rmfh = self.rmm.openFile(self.getDbTablePath(relName))
            self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
            rid = Rid(1, -1)
            while True:
                record = self.rm_scan.getNextRecord(rid)
                rid = record.rid
                if rid == Rid(-1, -1):
                    break
                count += 1
                data = record.data
                attr = U_unpack_attr(data[offset:offset+length], typ, length)
                if attr in pool:
                    raiseerr = True
                    break
                pool.add(attr)
            self.rm_scan.closeScan()
            self.rmm.closeFile(rmfh)
        if raiseerr:
            raise myErr("AddUniqueError", "The column has repeated value.")
        info["unique"] = 1
        record = RM_Record(U_pack_attrcat_data(info), rid_)
        self.attr_fhandle.updateRecord(record)
        if not there_is_index:
            self.CreateIndex(relName, attrName)

    def check_if_pk_is_repeated(self, relName, ini_value_list, comp_value:bool):
        '''
        comp_value是什么时候停止，检查主键重复传入True，检查外键refer主键是否存在传入False
        结果统一：不好的是True，好的是False
        '''


        value_list = list(set(ini_value_list))

        if comp_value and len(value_list) != len(ini_value_list):
            return True

        ixfh = self.ixm.openIndex(self.getDbTablePath(relName), 0)
        all_repeated = not comp_value
        for value in value_list:
            self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, value)
            repeated, value, rid = self.ix_scan.getNextEntry()
            self.ix_scan.closeScan()
            if repeated == comp_value:
                all_repeated = repeated
                break
            
        self.ixm.closeIndex(ixfh)
        return all_repeated == comp_value

    def check_if_unique_is_repeated(self, relName, indexNo, ini_value_list):
        value_list = list(set(ini_value_list))
        if len(value_list) != len(ini_value_list):
            return True
        
        ixfh = self.ixm.openIndex(self.getDbTablePath(relName), indexNo)
        all_repeated = False

        for value in value_list:
            self.ix_scan.openScan(ixfh, COMP_OP.EQ_OP, value)
            repeated, value, rid = self.ix_scan.getNextEntry()
            self.ix_scan.closeScan()
            if repeated:
                all_repeated = True
                break

        self.ixm.closeIndex(ixfh)
        return all_repeated

    @check_db_open
    def Aggregator_with_where(self, relName, attrName, Ag_OP:int, rid_list):
        # sourcery skip: assign-if-exp, merge-comparisons, merge-else-if-into-elif
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
        if Ag_OP == Aggregator.COUNT and attrName == "*":
            return len(rid_list)
        exist, rid_, info = self.getAttrInfoFromCat(relName, attrName)
        if not exist:
            raise myErr("AttrError", f"There isn't an attr named {attrName} on {relName}.")
        if info["attrType"] == ATTR_TYPE.STRING and Ag_OP != Aggregator.COUNT:
            raise myErr("AggregatorError", f"The attr {attrName} doesn't support the aggregator function.")
        count_ = 0
        sum_ = None
        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        offset = info["offset"]
        rank = info["rank"]
        typ = info["attrType"]
        length = info["attrLength"]

        for rid in rid_list:
            record = rmfh.getRecord(rid)
            if not U_check_if_attr_is_null(record.data, rank):
                count_ += 1
                if Ag_OP == Aggregator.COUNT:
                    continue
                value = U_unpack_attr(record.data[offset:offset+length], typ, length)
                if Ag_OP == Aggregator.SUM or Ag_OP == Aggregator.AVERAGE:
                    if sum_ is None:
                        sum_ = value
                    else:
                        sum_ += value
                elif Ag_OP == Aggregator.MAX:
                    if sum_ is None:
                        sum_ = value
                    else:
                        sum_ = max(sum_, value)
                else:
                    if sum_ is None:
                        sum_ = value
                    else:
                        sum_ = min(sum_, value)

            
        self.rmm.closeFile(rmfh)
        if Ag_OP == Aggregator.COUNT:
            return count_
        if Ag_OP == Aggregator.SUM or Ag_OP == Aggregator.MAX or Ag_OP == Aggregator.MIN:
            return "NULL" if count_ == 0 else sum_
        if Ag_OP == Aggregator.AVERAGE:
            return "NULL" if count_ == 0 else sum_ / count_

    # @check_db_open
    # def Aggregator_without_where_Join(self, ):

    @check_db_open
    def Aggregator_without_where(self, relName, attrName, Ag_OP:int):
        # sourcery skip: merge-comparisons, merge-else-if-into-elif, swap-if-else-branches, swap-if-expression
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
        if Ag_OP == Aggregator.COUNT and attrName == "*": #返回所有的行数，包括NULL
            count = 0
            if os.path.exists(self.getDbTablePath(f"{relName}.0")): #有主键
                ixfh = self.ixm.openIndex(self.getDbTablePath(relName), 0)
                self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
                while True:
                    find, value, rid = self.ix_scan.getNextEntry()
                    if not find:
                        break
                    count += 1
                self.ix_scan.closeScan()
                self.ixm.closeIndex(ixfh)
            else:
                rmfh = self.rmm.openFile(self.getDbTablePath(relName))
                self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                rid = Rid(1, -1)
                while True:
                    record = self.rm_scan.getNextRecord(rid)
                    rid = record.rid
                    if rid == Rid(-1, -1):
                        break
                    count += 1
                self.rm_scan.closeScan()
                self.rmm.closeFile(rmfh)
            return count
        exist, rid_, info = self.getAttrInfoFromCat(relName, attrName)
        if not exist:
            raise myErr("AttrError", f"There isn't an attr named {attrName} on {relName}.")
        if info["attrType"] == ATTR_TYPE.STRING and Ag_OP != Aggregator.COUNT:
            raise myErr("AggregatorError", f"The attr {attrName} doesn't support the aggregator function.")

        if Ag_OP == Aggregator.COUNT or Ag_OP == Aggregator.SUM or Ag_OP == Aggregator.AVERAGE:
            count = 0
            data_num = 0
            if info["indexNo"] != -1: #有索引
                ixfh = self.ixm.openIndex(self.getDbTablePath(relName), info["indexNo"])
                self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
                while True:
                    find, value, rid = self.ix_scan.getNextEntry()
                    if not find:
                        break
                    count += 1
                    if Ag_OP == Aggregator.SUM or Ag_OP == Aggregator.AVERAGE:
                        data_num += value
                self.ix_scan.closeScan()
                self.ixm.closeIndex(ixfh)
            else:
                rmfh = self.rmm.openFile(self.getDbTablePath(relName))
                self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                offset = info["offset"]
                length = info["attrLength"]
                typ = info["attrType"]
                rank = info["rank"]
                rid = Rid(1, -1)
                while True:
                    record = self.rm_scan.getNextRecord(rid)
                    rid = record.rid
                    if rid == Rid(-1, -1):
                        break
                    if U_check_if_attr_is_null(record.data, rank): #是null值
                        continue
                    count += 1
                    if Ag_OP == Aggregator.SUM or Ag_OP == Aggregator.AVERAGE:
                        value = U_unpack_attr(record.data[offset:offset+length], typ, length)
                        data_num += value
                self.rm_scan.closeScan()
                self.rmm.closeFile(rmfh)

            if Ag_OP == Aggregator.COUNT:
                return count
            elif Ag_OP == Aggregator.SUM:
                return "NULL" if count == 0 else data_num
            else:
                return "NULL" if count == 0 else data_num / count
        else:

            if info["indexNo"] != -1:
                if Ag_OP == Aggregator.MIN:
                    ixfh = self.ixm.openIndex(self.getDbTablePath(relName), info["indexNo"])
                    self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
                    find, value, rid = self.ix_scan.getNextEntry()
                    self.ix_scan.closeScan()
                    self.ixm.closeIndex(ixfh)
                    return "NULL" if not find else value
                else:
                    ixfh = self.ixm.openIndex(self.getDbTablePath(relName), info["indexNo"])
                    self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
                    max_v = None
                    while True:
                        find, value, rid = self.ix_scan.getNextEntry()
                        if not find:
                            break
                        max_v = value
                    self.ix_scan.closeScan()
                    self.ixm.closeIndex(ixfh)
                    return "NULL" if max_v is None else max_v
            else:
                end_value = None
                rmfh = self.rmm.openFile(self.getDbTablePath(relName))
                self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
                offset = info["offset"]
                length = info["attrLength"]
                typ = info["attrType"]
                rank = info["rank"]
                rid = Rid(1, -1)
                while True:
                    record = self.rm_scan.getNextRecord(rid)
                    rid = record.rid
                    if rid == Rid(-1, -1):
                        break
                    if U_check_if_attr_is_null(record.data, rank): #是null值
                        continue
                    value = U_unpack_attr(record.data[offset:offset+length], typ, length)
                    if end_value is None:
                        end_value = value
                    elif Ag_OP == Aggregator.MAX:
                        end_value = max(end_value, value)
                    else:
                        end_value = min(end_value, value)
                self.rm_scan.closeScan()
                self.rmm.closeFile(rmfh)
                return "NULL" if end_value is None else end_value

    @check_db_open
    def Process_where_and_clause_Join(self, relName_List, where_clause_list):
        for relName in relName_List:
            if not os.path.exists(self.getDbTablePath(relName)):
                raise myErr("TableError", f"There isn't a table named {relName}.")
        if len(relName_List) == 1:
            for i in range(len(where_clause_list)):
                where_clause_list[i][1] = where_clause_list[i][1][1]
                if where_clause_list[i][0] <= 5 and where_clause_list[i][2][0] == False:
                    #column op column的格式 默认不检查tablename
                    where_clause_list[i][2][1] = where_clause_list[i][2][1][1]
            return False, self.Process_where_and_clause(relName_List[0], where_clause_list), [], []

        A_clause_list = []
        B_clause_list = []
        C_clause_list = [] #针对 a.column op b.column的情况，储存op a.column b.column
        for clause in where_clause_list:
            if clause[1][0] == relName_List[0]:
                clause[1] = clause[1][1]
                if clause[0] <= 5 and clause[2][0] == False:
                    if clause[2][1][0] == relName_List[0]:
                        clause[2][1] = clause[2][1][1]
                        A_clause_list.append(clause)
                    elif clause[2][1][0] == relName_List[1]:
                        C_clause_list.append([clause[0], clause[1], clause[2][1][1]])
                    else:
                        raise myErr("TableError", "The table name in where clause is wrong.")
                else:
                    A_clause_list.append(clause)
            elif clause[1][0] == relName_List[1]:
                clause[1] = clause[1][1]
                if clause[0] <= 5 and clause[2][0] == False:
                    if clause[2][1][0] == relName_List[1]:
                        clause[2][1] = clause[2][1][1]
                        B_clause_list.append(clause)
                    elif clause[2][1][0] == relName_List[0]:
                        op = clause[0]
                        if op == COMP_OP.GE_OP:
                            op = COMP_OP.LE_OP
                        elif op == COMP_OP.LE_OP:
                            op = COMP_OP.GE_OP
                        elif op == COMP_OP.GT_OP:
                            op = COMP_OP.LT_OP
                        elif op == COMP_OP.LT_OP:
                            op = COMP_OP.GT_OP
                        C_clause_list.append([op, clause[2][1][1], clause[1]]) #这里得反向大于小于号
                    else:
                        raise myErr("TableError", "The table name in where clause is wrong.")
                else:
                    B_clause_list.append(clause)
            else:
                raise myErr("TableError", "The table name in where-clause is not consistent with you gave.")
        a_rid_list = []
        b_rid_list = []
        #返回的第一个参数表示 rid_list是不是真的(如果是假的，那么[]表示all，否则就是真的空)
        if A_clause_list:
            a_rid_list = self.Process_where_and_clause(relName_List[0], A_clause_list)
            if len(a_rid_list) == 0:
                return True, [], [], []
        if B_clause_list:
            b_rid_list = self.Process_where_and_clause(relName_List[1], B_clause_list)
            if len(b_rid_list) == 0:
                return True, [], [], []

        return False, a_rid_list, b_rid_list, C_clause_list


    @check_db_open
    def Process_where_and_clause(self, relName, where_clause_list):
        # sourcery skip: assign-if-exp, swap-if-expression
        if not os.path.exists(self.getDbTablePath(relName)):
            raise myErr("TableError", f"There isn't a table named {relName}.")
            
        attrInfos = self.getAllAttrInfoFromCat(relName)
        for where_clause in where_clause_list:
            #基本的格式是[op, colname, ...]
            for attrinfo in attrInfos:
                if attrinfo["attrName"] == where_clause[1]:
                    where_clause[1] = [attrinfo["attrType"], attrinfo["attrLength"], attrinfo["offset"], attrinfo["indexNo"], attrinfo["rank"]]
                    if where_clause[0] <= 5 and where_clause[2][0] == False: #column op column的格式
                        for attrinfo_ in attrInfos:
                            if attrinfo_["attrName"] == where_clause[2][1]:
                                where_clause[2][1] = [attrinfo_["attrType"], attrinfo_["attrLength"], attrinfo_["offset"], attrinfo_["rank"]]
                                break
                        if not isinstance(where_clause[2][1], list):
                            raise myErr("AttrError", "You gave a wrong attr.")
                        if not (attrinfo["attrType"] == attrinfo_["attrType"] or attrinfo["attrType"] + attrinfo_["attrType"] == 3):
                            raise myErr("AttrError", "The two column can't be compared.")
                    if where_clause[0] <= 5 and where_clause[2][0] == True: #column op value的格式
                        if where_clause[1][0] == ATTR_TYPE.FLOAT and where_clause[2][2] == ATTR_TYPE.STRING:
                            raise myErr("ValueError", "The value in value list is a wrong type.")
                        elif where_clause[1][0] == ATTR_TYPE.INT and where_clause[2][2] != ATTR_TYPE.INT:
                            raise myErr("ValueError", "The value in value list is a wrong type.")
                        elif where_clause[1][0] == ATTR_TYPE.STRING and where_clause[2][2] != ATTR_TYPE.STRING:
                            raise myErr("ValueError", "The value in value list is a wrong type.")
                    if where_clause[0] == WhereClause.LIKE:
                        if attrinfo["attrType"] != ATTR_TYPE.STRING:
                            raise myErr("AttrError", "The Like select can be only on VARCHAR type.")
                        where_clause[2] = U_change_LIKESTRING_to_re(where_clause[2])
                    break
            if not isinstance(where_clause[1], list):
                raise myErr("AttrError", "You gave a wrong attr.")

        first_search = next(
            (
                i
                for i, where_clause in enumerate(where_clause_list)
                if where_clause[1][3] != -1
                and (
                    where_clause[0] == WhereClause.IS_NOT_NULL
                    or where_clause[0] == WhereClause.IS_NULL
                    or (where_clause[0] != 1 and where_clause[0] <= 5 and where_clause[2][0] == True)
                )
            ),
            0,
        )
        search_order = list(range(len(where_clause_list)))
        search_order[0] = first_search
        search_order[first_search] = 0

        #先搜第一个：
        rid_list = []
        first_clause = where_clause_list[search_order[0]]
        if first_clause[1][3] != -1 and first_clause[0] == WhereClause.IS_NOT_NULL:
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), first_clause[1][3])
            self.ix_scan.openScan(ixfh, COMP_OP.NO_OP)
            while True:
                find, value, rid = self.ix_scan.getNextEntry()
                if not find:
                    break
                rid_list.append(rid)
            self.ix_scan.closeScan()
            self.ixm.closeIndex(ixfh)
        elif first_clause[1][3] != -1 and first_clause[0] == WhereClause.IS_NULL:
            rmfh = self.rmm.openFile(self.getDbTablePath(f"{relName}.index{first_clause[1][3]}null"))
            self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
            rid = Rid(1, -1)
            while True:
                record = self.rm_scan.getNextRecord(rid)
                rid = record.rid
                if rid == Rid(-1, -1):
                    break
                rid_arr = U_unpack_attr_array(record.data, ATTR_TYPE.INT, 4, 0, 2)
                rid_list.append(Rid(rid_arr[0], rid_arr[1]))
            self.rm_scan.closeScan()
            self.rmm.closeFile(rmfh)
        elif first_clause[1][3] != -1 and first_clause[0] != 1 and first_clause[0] <= 5 and first_clause[2][0] == True:
            ixfh = self.ixm.openIndex(self.getDbTablePath(relName), first_clause[1][3])
            self.ix_scan.openScan(ixfh, first_clause[0], first_clause[2][1])
            while True:
                find, value, rid = self.ix_scan.getNextEntry()
                if not find:
                    break
                rid_list.append(rid)
            self.ix_scan.closeScan()
            self.ixm.closeIndex(ixfh)
        else:
            rmfh = self.rmm.openFile(self.getDbTablePath(relName))
            self.rm_scan.openScan(rmfh, ATTR_TYPE.INT, 4, 0, STRING_EQUAL_FUNC, None)
            rid = Rid(1, -1)
            while True:
                record = self.rm_scan.getNextRecord(rid)
                rid = record.rid
                if rid == Rid(-1, -1):
                    break
                need_add = False
                is_null = U_check_if_attr_is_null(record.data, first_clause[1][4])
                if not is_null:
                    main_value = U_unpack_attr(record.data[first_clause[1][2]:first_clause[1][2]+first_clause[1][1]], first_clause[1][0], first_clause[1][1])
                else:
                    main_value = None
                if first_clause[0] == WhereClause.IS_NOT_NULL:
                    need_add = not is_null
                elif first_clause[0] == WhereClause.IS_NULL:
                    need_add = is_null
                elif first_clause[0] == WhereClause.IN:
                    if is_null:
                        if ATTR_TYPE.NONETYPE in first_clause[3]:
                            need_add = True
                        else:
                            need_add = False
                    else:
                        for i, value in enumerate(first_clause[2]):
                            typ = first_clause[3][i]
                            if first_clause[1][0] == typ or (first_clause[1][0] == ATTR_TYPE.FLOAT and typ == ATTR_TYPE.INT):
                                if main_value == value:
                                    need_add = True
                                    break
                elif first_clause[0] == WhereClause.LIKE:
                    if not is_null:
                        if re.fullmatch(first_clause[2], main_value) is not None:
                            need_add = True
                elif first_clause[2][0] == True:
                    if not is_null:
                        if first_clause[0] == WhereClause.EQ:
                            if main_value == first_clause[2][1]:
                                need_add = True
                        elif first_clause[0] == WhereClause.GE:
                            if main_value >= first_clause[2][1]:
                                need_add = True
                        elif first_clause[0] == WhereClause.GT:
                            if main_value > first_clause[2][1]:
                                need_add = True
                        elif first_clause[0] == WhereClause.LT:
                            if main_value < first_clause[2][1]:
                                need_add = True
                        elif first_clause[0] == WhereClause.LE:
                            if main_value <= first_clause[2][1]:
                                need_add = True
                        else:
                            if main_value != first_clause[2][1]:
                                need_add = True
                else:
                    if not is_null:
                        if not U_check_if_attr_is_null(record.data, first_clause[2][1][3]):
                            vice_value = U_unpack_attr(record.data[first_clause[2][1][2]:first_clause[2][1][2]+first_clause[2][1][1]], first_clause[2][1][0], first_clause[2][1][1])
                            if first_clause[0] == WhereClause.EQ:
                                if main_value == vice_value :
                                    need_add = True
                            elif first_clause[0] == WhereClause.GE:
                                if main_value >= vice_value :
                                    need_add = True
                            elif first_clause[0] == WhereClause.GT:
                                if main_value > vice_value :
                                    need_add = True
                            elif first_clause[0] == WhereClause.LT:
                                if main_value < vice_value :
                                    need_add = True
                            elif first_clause[0] == WhereClause.LE:
                                if main_value <= vice_value :
                                    need_add = True
                            else:
                                if main_value != vice_value :
                                    need_add = True
                if need_add:
                    rid_list.append(rid)
            self.rm_scan.closeScan()
            self.rmm.closeFile(rmfh)

        if len(rid_list) == 0 or len(search_order) == 1:
            return rid_list

        rmfh = self.rmm.openFile(self.getDbTablePath(relName))
        end_rid_list = []
        for rid in rid_list:
            record = rmfh.getRecord(rid)
            need_add = True
            
            for order in search_order[1:]:
                clause = where_clause_list[order]
                
                is_null = U_check_if_attr_is_null(record.data, clause[1][4])
                if not is_null:
                    main_value = U_unpack_attr(record.data[clause[1][2]:clause[1][2]+clause[1][1]], clause[1][0], clause[1][1])
                else:
                    main_value = None

                if clause[0] == WhereClause.IS_NOT_NULL:
                    if is_null:
                        need_add = False
                        break
                elif clause[0] == WhereClause.IS_NULL:
                    if not is_null:
                        need_add = False
                        break
                elif clause[0] == WhereClause.IN:
                    if is_null:
                        if ATTR_TYPE.NONETYPE not in clause[3]:
                            need_add = False
                            break
                    else:
                        in_list = False
                        for i, value in enumerate(clause[2]):
                            typ = clause[3][i]
                            if clause[1][0] == typ or (clause[1][0] == ATTR_TYPE.FLOAT and typ == ATTR_TYPE.INT):
                                if main_value == value:
                                    in_list = True
                                    break
                        if not in_list:
                            need_add = False
                            break
                elif clause[0] == WhereClause.LIKE:
                    if not is_null:
                        if re.fullmatch(clause[2], main_value) is not None:
                            continue
                elif clause[2][0] == True:
                    if is_null:
                        need_add = False
                        break
                    else:
                        if clause[0] == WhereClause.EQ:
                            if main_value != clause[2][1]:
                                need_add = False
                                break
                        elif clause[0] == WhereClause.GE:
                            if main_value < clause[2][1]:
                                need_add = False
                                break
                        elif clause[0] == WhereClause.GT:
                            if main_value <= clause[2][1]:
                                need_add = False
                                break
                        elif clause[0] == WhereClause.LT:
                            if main_value >= clause[2][1]:
                                need_add = False
                                break
                        elif clause[0] == WhereClause.LE:
                            if main_value > clause[2][1]:
                                need_add = False
                                break
                        else:
                            if main_value == clause[2][1]:
                                need_add = False
                                break
                else:
                    if not is_null:
                        if not U_check_if_attr_is_null(record.data, clause[2][1][3]):
                            vice_value = U_unpack_attr(record.data[clause[2][1][2]:clause[2][1][2]+clause[2][1][1]], clause[2][1][0], clause[2][1][1])
                            if clause[0] == WhereClause.EQ:
                                if main_value == vice_value :
                                    continue
                            elif clause[0] == WhereClause.GE:
                                if main_value >= vice_value :
                                    continue
                            elif clause[0] == WhereClause.GT:
                                if main_value > vice_value :
                                    continue
                            elif clause[0] == WhereClause.LT:
                                if main_value < vice_value :
                                    continue
                            elif clause[0] == WhereClause.LE:
                                if main_value <= vice_value :
                                    continue
                            else:
                                if main_value != vice_value :
                                    continue
                    need_add = False
                    break

            if need_add:
                end_rid_list.append(rid)

        self.rmm.closeFile(rmfh)
        return end_rid_list


        