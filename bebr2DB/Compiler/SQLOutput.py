from prettytable import PrettyTable
from ..settings import *
from textwrap import fill

class SQLOutput():
    def __init__(self, message, there_is_table:bool, field_name=None, row_lists=None, is_row=True, following_msg=None) -> None:
        '''
        如果is_row = True，field_name是一维列表，row_lists是二维列表
        否则field_name是字符串，row_lists是一维列表
        '''
        self.msg = message
        self.there_is_table = there_is_table
        self.field_name = field_name
        self.row_lists = row_lists
        self.is_row = is_row
        self.following_msg = following_msg

        if isinstance(field_name, list):
            field_name_set = set()
            for i, k in enumerate(self.field_name):
                if k in field_name_set:
                    self.field_name[i] = k + " " * i
                else:
                    field_name_set.add(k)

    def web_return(self):
        if not self.there_is_table:
            return [False, self.msg]
        if not self.is_row:
            self.field_name = self.field_name.replace(".", "-")
            return [True, [self.field_name], [{self.field_name:rl} for rl in self.row_lists], self.msg, self.following_msg]
        di = []
        self.field_name = [n.replace(".", "-") for n in self.field_name]
        rls = self.row_lists
        if len(self.row_lists) > 1000000:
            rls = self.row_lists[:1000000]
            if self.following_msg is not None:
                self.following_msg += f"\n总记录数是{len(self.row_lists)}，由于数量过多只显示前百万条记录。"
            else:
                self.following_msg = f"\n总记录数是{len(self.row_lists)}，由于数量过多只显示前百万条记录。"
        for rl in rls:
            a = {fn: round(rl[i],4) if isinstance(rl[i], float) else rl[i] for i, fn in enumerate(self.field_name)}
            di.append(a)
        return [True, self.field_name, di, self.msg, self.following_msg]

    def print(self):  # sourcery skip: hoist-statement-from-if
        if self.msg is not None and self.msg != "":
            print(self.msg)
        if self.there_is_table:
            try:
                tb = PrettyTable()
                
                rows_count = len(self.row_lists)
                if self.is_row:
                    wi = 120//len(self.field_name)
                    tb.field_names = self.field_name
                    for l in self.row_lists:
                        #默认是两维列表
                        new_l = [fill(str(round(l_,4) if isinstance(l_, float) else l_), width=wi) for l_ in l]
                        tb.add_row(new_l)
                else:
                    #这个时候，默认只有一列了，一维列表
                    tb.add_column(self.field_name, self.row_lists)
                print(tb)
                if self.following_msg is not None and self.following_msg != "":
                    if self.following_msg.endswith("\n"):
                        print(self.following_msg[:-1])
                    else:
                        print(self.following_msg)
                print(f'{bcolors.OKBLUE}{rows_count} {"rows" if rows_count > 1 else "row"} in total.{bcolors.ENDC}')
            except:
                print(" ".join(self.field_name))
                rows_count = len(self.row_lists)
                for l in self.row_lists:
                    #默认是两维列表
                    print(l)
                if self.following_msg is not None and self.following_msg != "":
                    print(self.following_msg)
                print(f'{bcolors.OKBLUE}{rows_count} {"rows" if rows_count > 1 else "row"} in total.{bcolors.ENDC}')
