# Generated from D:\bebr2DB\bebr2DB\Compiler\SQL.g4 by ANTLR 4.11.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SQLParser import SQLParser
else:
    from SQLParser import SQLParser

from ..SystemManager import SM_Manager
import time
from ..settings import *
from .SQLOutput import SQLOutput
from ..Error import myErr
import os

# This class defines a complete generic visitor for a parse tree produced by SQLParser.

class SQLVisitor(ParseTreeVisitor):

    def __init__(self, smm: SM_Manager) -> None:
        super().__init__()
        self.smm = smm

    def aggregateResult(self, aggregate, nextResult):
        if nextResult is None:
            return aggregate
        elif isinstance(nextResult, list) and isinstance(aggregate, list):
            if isinstance(aggregate[0], list):
                aggregate.append(nextResult)
                return aggregate
            return [aggregate, nextResult]
        else:
            return nextResult
        # return aggregate if nextResult is None else nextResult

    def web_visit(self, ctx:SQLParser.ProgramContext):
        start_ = time.perf_counter()
        try:
            result = self.visitChildren(ctx)
        except Exception as e:
            err = e.__str__()
            err = err.replace("\033[91m", "").replace('\033[0m', "")
            return [False, err, self.smm.dbName]
        end_ = time.perf_counter()
        if result is not None:
            return [True, end_-start_, self.smm.dbName, result.web_return()]
        else:
            return [True, end_-start_, self.smm.dbName]

    # Visit a parse tree produced by SQLParser#program.
    def visitProgram(self, ctx:SQLParser.ProgramContext):
        start_ = time.perf_counter()
        try:
            result = self.visitChildren(ctx)
        except Exception as e:
            print(e)
        end_ = time.perf_counter()
        try:
            result.print()
        except:
            pass
        print(f'{bcolors.OKBLUE}'+'Finished in {:.3f} sec.'.format(end_-start_) +f'{bcolors.ENDC}')


    # Visit a parse tree produced by SQLParser#statement.
    def visitStatement(self, ctx:SQLParser.StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#create_db.
    def visitCreate_db(self, ctx:SQLParser.Create_dbContext):
        dbName = ctx.Identifier().getText()
        self.smm.CreateDb(dbName)
        return SQLOutput(f"Successfully create the database {dbName}.", False)


    # Visit a parse tree produced by SQLParser#drop_db.
    def visitDrop_db(self, ctx:SQLParser.Drop_dbContext):
        dbName = ctx.Identifier().getText()
        self.smm.DropDb(ctx.Identifier().getText())
        return SQLOutput(f"Successfully delete the database {dbName}.", False)


    # Visit a parse tree produced by SQLParser#show_dbs.
    def visitShow_dbs(self, ctx:SQLParser.Show_dbsContext):
        l = self.smm.ShowDbs()
        return SQLOutput("All Databases:", True, "Name", l, False)


    # Visit a parse tree produced by SQLParser#use_db.
    def visitUse_db(self, ctx:SQLParser.Use_dbContext):
        self.smm.OpenDb(ctx.Identifier().getText())
        return


    # Visit a parse tree produced by SQLParser#show_tables.
    def visitShow_tables(self, ctx:SQLParser.Show_tablesContext):
        if not self.smm.open:
            return SQLOutput("Please open a database first.", False)
        nl = self.smm.getAllTableName()
        return SQLOutput("All tables:", True, "Name", nl, False)


    # Visit a parse tree produced by SQLParser#show_indexes.
    def visitShow_indexes(self, ctx:SQLParser.Show_indexesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#load_data.
    def visitLoad_data(self, ctx:SQLParser.Load_dataContext):
        string = ctx.String().getText()[1:-1]
        table_name = ctx.Identifier().getText()
        vl = []
        with open(os.path.join(os.getcwd(), string), "r") as f:
            lines = f.readlines()
            for line in lines:
                value = line.rstrip().split(',')
                vl.append(value)
        self.smm.LoadData(table_name, vl)
        return SQLOutput("Successfully loaded.", False)


    # Visit a parse tree produced by SQLParser#dump_data.
    def visitDump_data(self, ctx:SQLParser.Dump_dataContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#create_table.
    def visitCreate_table(self, ctx:SQLParser.Create_tableContext):
        tb_name = ctx.Identifier().getText()
        if tb_name.find(".") != -1:
            raise myErr("IllegalName", "Bebr2DB don't allow name with '.'.")
        field_list = self.visitChildren(ctx)
        if not isinstance(field_list[0], list):
            field_list = [field_list]
        attrInfos = []
        pk_name = None
        fk_list = []
        for f in field_list:
            if len(f) == 6:
                attrInfos.append({"name":f[0], "type":f[1], "length":f[2], "null":f[3], "default":f[4], "default_is_null":f[5]})
            elif len(f) == 1:
                if pk_name is not None:
                    raise myErr("PKError", "You can't create two primary key on one table.")
                pk_name = f[0]
            elif len(f) == 4:
                fk_list.append(f)

        self.smm.CreateTable(tb_name, attrInfos, pk_name, fk_list)
        return SQLOutput(f"Successfully create the table {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#drop_table.
    def visitDrop_table(self, ctx:SQLParser.Drop_tableContext):
        tb_name = ctx.Identifier().getText()
        self.smm.DropTable(tb_name)
        return SQLOutput(f"Successfully dropped the table {tb_name}.", False)



    # Visit a parse tree produced by SQLParser#describe_table.
    def visitDescribe_table(self, ctx:SQLParser.Describe_tableContext):
        res, pk, unique, index, fk_list = self.smm.Describe(ctx.Identifier().getText())
        f_msg = ""
        if pk is not None:
            f_msg += f"PRIMARY KEY ({pk});\n"
        if fk_list:
            for f in fk_list:
                f_msg += f"FOREIGN KEY {f[0]}({f[1]}) REFERENCES {f[2]}({f[3]});\n"
        if unique:
            for u in unique:
                f_msg += f"UNIQUE ({u});\n"
        if index:
            for i in index:
                f_msg += f"INDEX ({i});\n"
        return SQLOutput("Table info:", True, ["Field", "Type", "Null", "Default"], res, True, f_msg)


    # Visit a parse tree produced by SQLParser#insert_into_table.
    def visitInsert_into_table(self, ctx:SQLParser.Insert_into_tableContext):
        tb_name = ctx.Identifier().getText()
        l = self.visitValue_lists(ctx.value_lists())
        # value_list = []
        # type_list = []
        # for v in l:
        #     s = v[0]
        #     if s == "NULL":
        #         type_list.append(ATTR_TYPE.NONETYPE)
        #         value_list.append(s)
        #     elif len(s) > 2 and s[0] == "'" and s[-1] == "'":
        #         type_list.append(ATTR_TYPE.STRING)
        #         value_list.append(s[1:-1])
        #     elif s.find(".") == -1:
        #         type_list.append(ATTR_TYPE.INT)
        #         value_list.append(int(s))
        #     else:
        #         type_list.append(ATTR_TYPE.FLOAT)
        #         value_list.append(float(s))
        self.smm.Insert_RoutineWork(tb_name, l)
        return SQLOutput("Successfully inserted.", False)


    # Visit a parse tree produced by SQLParser#delete_from_table.
    def visitDelete_from_table(self, ctx:SQLParser.Delete_from_tableContext):
        table_name = ctx.Identifier().getText()
        where_clause_list = self.visitWhere_and_clause(ctx.where_and_clause())
        #因为是单表，所以不会有a.column op b.column，yeah！！！
        is_true, rid_list_1, rid_list_2, cross_clause_list = self.smm.Process_where_and_clause_Join([table_name], where_clause_list)
        nums = self.smm.Delete(table_name, rid_list_1)
        return SQLOutput(f"Successfully deleted {nums} {'records' if nums > 1 else 'record'} on {table_name}.", False)


    # Visit a parse tree produced by SQLParser#update_table.
    def visitUpdate_table(self, ctx:SQLParser.Update_tableContext):
        set_clause_list = self.visitSet_clause(ctx.set_clause())
        table_name = ctx.Identifier().getText()
        where_clause_list = self.visitWhere_and_clause(ctx.where_and_clause())
        is_true, rid_list_1, rid_list_2, cross_clause_list = self.smm.Process_where_and_clause_Join([table_name], where_clause_list)
        self.smm.Update(table_name, set_clause_list, rid_list_1)
        return SQLOutput(f"Successfully updated {len(rid_list_1)} {'records' if len(rid_list_1) > 1 else 'record'} on {table_name}.", False)

    # Visit a parse tree produced by SQLParser#select_table_.
    def visitSelect_table_(self, ctx:SQLParser.Select_table_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#select_table.
    def visitSelect_table(self, ctx:SQLParser.Select_tableContext):
        # sourcery skip: assign-if-exp, hoist-similar-statement-from-if, merge-comparisons, merge-else-if-into-elif, remove-redundant-pass, simplify-len-comparison, swap-nested-ifs
        col_names = []
        aggr_list = []
        select_list = self.visitSelectors(ctx.selectors())

        if select_list is None:
            col_names.append("*")
        else:
            for selector in select_list:
                if selector[0] == Aggregator.NO:
                    col_names.append(selector[1])
                else:
                    aggr_list.append(selector)
        tb_name_list = self.visitIdentifiers(ctx.identifiers())
        if len(tb_name_list) > 2:
            raise myErr("NotSupport", "We don't support three or more table names joined.")
        # if len(tb_name_list) == 1:
        #     tb_name = tb_name_list[0]
        #GROUP BY
        group_by_column = None
        if ctx.column() is not None:
            if len(aggr_list) == 0 or len(col_names) != 0:
                raise myErr("GroupByError", "Please use group by with aggregator function.")
            #group by 会全表查询的，所以性能会下降
            group_by_column = self.visitColumn(ctx.column())
        #LIMIT OFFSET
        limit = None
        offset = 0
        if ctx.Integer(0) is not None:
            if group_by_column is not None:
                raise myErr("LimitError", "You can't use Groupby and Limit(Offset) at the same time.")
            if len(aggr_list) > 0:
                raise myErr("LimitError", "You can't use Aggregator Function and Limit(Offset) at the same time.")
            limit = int(ctx.Integer(0).getText())
            if ctx.Integer(1) is not None:
                offset = int(ctx.Integer(1).getText())

        if ctx.where_and_clause() is None:
            if len(aggr_list) == 0: #没有聚合函数
                if col_names[0] == "*":
                    colname, res = self.smm.Select_Join(tb_name_list, [], limit, offset)
                else:
                    colname, res = self.smm.Select_Join(tb_name_list, col_names, limit, offset)
                return SQLOutput("", True, colname, res)
            else: #有聚合函数，其他就不管了
                aggr_result_list = []
                group_name = None

                if len(tb_name_list) == 1 and group_by_column is None:
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            aggr_result = self.smm.Aggregator_without_where(tb_name_list[0], aggr[1], aggr[0])
                        else:
                            aggr_result = self.smm.Aggregator_without_where(tb_name_list[0], aggr[1][1], aggr[0])
                        aggr_result_list.append(aggr_result)
                elif group_by_column is not None:
                    attr_name, res = self.smm.Select_Join(tb_name_list, [])
                    group_id = -1
                    if len(tb_name_list) == 1:
                        for i, attr in enumerate(attr_name):
                            if group_by_column[1] == attr:
                                group_id = i
                                group_name = attr
                                break
                    else:
                        for i, attr in enumerate(attr_name):
                            if f"{group_by_column[0]}.{group_by_column[1]}" == attr:
                                group_id = i
                                group_name = attr
                                break
                    if group_id == -1:
                        raise myErr("GroupByError", "You gave a wrong groupby name.")
                    group_by_value_list = dict()
                    for res_ in res:
                        if res_[group_id] in group_by_value_list:
                            group_by_value_list[res_[group_id]].append(res_)
                        else:
                            group_by_value_list[res_[group_id]] = [res_]
                    
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            l = []
                            for key in group_by_value_list:
                                l.append(len(group_by_value_list[key]))
                            aggr_result_list.append(l)
                        else:
                            l = []
                            for key in group_by_value_list:
                                count_ = 0
                                sum_ = 0
                                has = False
                                for i, attr in enumerate(attr_name):
                                    if f"{aggr[1][0]}.{aggr[1][1]}" == attr:
                                        has = True
                                        for res_ in group_by_value_list[key]:
                                            if res_[i] != "NULL":
                                                count_ += 1
                                                if isinstance(res_[i], str) and aggr[0] != Aggregator.COUNT:
                                                    raise myErr("AggregatorError", f"The attr {aggr[1][1]} doesn't support the aggregator function.")
                                                if aggr[0] == Aggregator.SUM or aggr[0] == Aggregator.AVERAGE:
                                                    sum_ += res_[i]
                                                elif aggr[0] == Aggregator.MAX:
                                                    if count_ == 1:
                                                        sum_ = res_[i]
                                                    else:
                                                        sum_ = max(res_[i], sum_)
                                                elif aggr[0] == Aggregator.MIN:
                                                    if count_ == 1:
                                                        sum_ = res_[i]
                                                    else:
                                                        sum_ = min(res_[i], sum_)
                                        break
                                if not has:
                                    raise myErr("AttrError", "You gave a wrong attr name.")
                                if aggr[0] == Aggregator.COUNT:
                                    l.append(count_)
                                elif aggr[0] == Aggregator.AVERAGE:
                                    l.append("NULL" if count_ == 0 else sum_ / count_)
                                else:
                                    l.append("NULL" if count_ == 0 else sum_)
                            aggr_result_list.append(l)

                    aggr_result_list = list(map(list, zip(*aggr_result_list)))
                    for i, key in enumerate(group_by_value_list):
                        aggr_result_list[i] = [key] + aggr_result_list[i]

                else:
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            continue
                        if aggr[1][0] == tb_name_list[0]:
                            continue
                        elif aggr[1][0] == tb_name_list[1]:
                            continue
                        else:
                            raise myErr("TableError", "The table name in aggreator function is not consistent with what you gave.")
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            aggr_result = 0
                        else:
                            aggr_result = self.smm.Aggregator_without_where(aggr[1][0], aggr[1][1], aggr[0])
                        aggr_result_list.append(aggr_result)

                    a_count = self.smm.Aggregator_without_where(tb_name_list[0], "*", Aggregator.COUNT)
                    b_count = self.smm.Aggregator_without_where(tb_name_list[1], "*", Aggregator.COUNT)

                    if a_count * b_count == 0:
                        for i in range(len(aggr_list)):
                            if aggr_list[i][0] == Aggregator.COUNT:
                                aggr_result_list[i] = 0
                            else:
                                aggr_result_list[i] = "NULL"
                    else:
                        for i, aggr in enumerate(aggr_list):
                            if aggr[1] == "*":
                                aggr_result_list[i] = a_count * b_count
                            elif aggr[0] == Aggregator.COUNT or aggr[0] == Aggregator.SUM:
                                if aggr_result_list[i] != "NULL":
                                    count_ = a_count if aggr[1][0] == tb_name_list[1] else b_count
                                    aggr_result_list[i] *= count_

                output_field = []
                output_result = []
                aggr_num = 0
                should_tips = False
                if group_name is not None:
                    output_field.append(group_name)

                for selector in select_list:
                    if selector[0] == Aggregator.NO:
                        output_field.append(f"{selector[1][0]}.{selector[1][1]}" if selector[1][0] != "" else selector[1][1])
                        output_result.append("-")
                        should_tips = True
                    else:
                        output_result.append(aggr_result_list[aggr_num])
                        if selector[0] == Aggregator.COUNT:
                            selector_text = "COUNT"
                        elif selector[0] == Aggregator.AVERAGE:
                            selector_text = "AVG"
                        elif selector[0] == Aggregator.MAX:
                            selector_text = "MAX"
                        elif selector[0] == Aggregator.MIN:
                            selector_text = "MIN"
                        else:
                            selector_text = "SUM"
                        if selector[1] == "*":
                            tbn = "*"
                        else:
                            tbn = f"{selector[1][0]}.{selector[1][1]}" if selector[1][0] != "" else selector[1][1]
                        output_field.append(f"{selector_text}({tbn})")
                        aggr_num += 1
                
                if group_name is not None:
                    return SQLOutput("", True, output_field, aggr_result_list)
                else:
                    follow_msg = "Due to there is(are) aggregator function(s), the other column(s) is(are) not showed." if should_tips else ""
                    return SQLOutput("", True, output_field, [output_result], True, follow_msg)
        else:
            where_clause_list = self.visitWhere_and_clause(ctx.where_and_clause())

            is_true, rid_list_1, rid_list_2, cross_clause_list = self.smm.Process_where_and_clause_Join(tb_name_list, where_clause_list)

            if len(aggr_list) == 0: #没有聚合函数
                if col_names[0] == "*":
                    colname, res = self.smm.Select_with_WhereClause_Join(tb_name_list, [], rid_list_1, rid_list_2, is_true, cross_clause_list, limit, offset)
                else:
                    colname, res = self.smm.Select_with_WhereClause_Join(tb_name_list, col_names, rid_list_1, rid_list_2, is_true, cross_clause_list, limit, offset)
                return SQLOutput("", True, colname, res)
            else:
                aggr_result_list = []
                group_name = None
                if group_by_column is not None:
                    attr_name, res = self.smm.Select_with_WhereClause_Join(tb_name_list, [], rid_list_1, rid_list_2, is_true, cross_clause_list)
                    group_id = -1
                    if len(tb_name_list) == 1:
                        for i, attr in enumerate(attr_name):
                            if group_by_column[1] == attr:
                                group_id = i
                                group_name = attr
                                break
                    else:
                        for i, attr in enumerate(attr_name):
                            if f"{group_by_column[0]}.{group_by_column[1]}" == attr:
                                group_id = i
                                group_name = attr
                                break
                    if group_id == -1:
                        raise myErr("GroupByError", "You gave a wrong groupby name.")
                    group_by_value_list = dict()
                    for res_ in res:
                        if res_[group_id] in group_by_value_list:
                            group_by_value_list[res_[group_id]].append(res_)
                        else:
                            group_by_value_list[res_[group_id]] = [res_]
                    
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            l = []
                            for key in group_by_value_list:
                                l.append(len(group_by_value_list[key]))
                            aggr_result_list.append(l)
                        else:
                            l = []
                            for key in group_by_value_list:
                                count_ = 0
                                sum_ = 0
                                has = False
                                for i, attr in enumerate(attr_name):
                                    if f"{aggr[1][0]}.{aggr[1][1]}" == attr:
                                        has = True
                                        for res_ in group_by_value_list[key]:
                                            if res_[i] != "NULL":
                                                count_ += 1
                                                if isinstance(res_[i], str) and aggr[0] != Aggregator.COUNT:
                                                    raise myErr("AggregatorError", f"The attr {aggr[1][1]} doesn't support the aggregator function.")
                                                if aggr[0] == Aggregator.SUM or aggr[0] == Aggregator.AVERAGE:
                                                    sum_ += res_[i]
                                                elif aggr[0] == Aggregator.MAX:
                                                    if count_ == 1:
                                                        sum_ = res_[i]
                                                    else:
                                                        sum_ = max(res_[i], sum_)
                                                elif aggr[0] == Aggregator.MIN:
                                                    if count_ == 1:
                                                        sum_ = res_[i]
                                                    else:
                                                        sum_ = min(res_[i], sum_)
                                        break
                                if not has:
                                    raise myErr("AttrError", "You gave a wrong attr name.")
                                if aggr[0] == Aggregator.COUNT:
                                    l.append(count_)
                                elif aggr[0] == Aggregator.AVERAGE:
                                    l.append("NULL" if count_ == 0 else sum_ / count_)
                                else:
                                    l.append("NULL" if count_ == 0 else sum_)
                            aggr_result_list.append(l)

                    aggr_result_list = list(map(list, zip(*aggr_result_list)))
                    for i, key in enumerate(group_by_value_list):
                        aggr_result_list[i] = [key] + aggr_result_list[i]
                elif len(tb_name_list) == 1:
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            aggr_result = self.smm.Aggregator_with_where(tb_name_list[0], aggr[1], aggr[0], rid_list_1)
                        else:
                            aggr_result = self.smm.Aggregator_with_where(tb_name_list[0], aggr[1][1], aggr[0], rid_list_1)
                        aggr_result_list.append(aggr_result)
                elif len(cross_clause_list) == 0:
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            continue
                        if aggr[1][0] == tb_name_list[0]:
                            continue
                        elif aggr[1][0] == tb_name_list[1]:
                            continue
                        else:
                            raise myErr("TableError", "The table name in aggreator function is not consistent with what you gave.")
                    if is_true and (len(rid_list_1) == 0 or len(rid_list_2) == 0):
                        #说明没东西了
                        for aggr in aggr_list:
                            if aggr[0] == Aggregator.COUNT:
                                aggr_result_list.append(0)
                            else:
                                aggr_result_list.append("NULL")
                    else:
                        for aggr in aggr_list:
                            if aggr[1] == "*":
                                aggr_result = 0
                            else:
                                if aggr[1][0] == tb_name_list[0]:
                                    if not is_true and len(rid_list_1) == 0:
                                        aggr_result = self.smm.Aggregator_without_where(aggr[1][0], aggr[1][1], aggr[0])
                                    else:
                                        aggr_result = self.smm.Aggregator_with_where(aggr[1][0], aggr[1][1], aggr[0], rid_list_1)
                                else:
                                    if not is_true and len(rid_list_2) == 0:
                                        aggr_result = self.smm.Aggregator_without_where(aggr[1][0], aggr[1][1], aggr[0])
                                    else:
                                        aggr_result = self.smm.Aggregator_with_where(aggr[1][0], aggr[1][1], aggr[0], rid_list_2)
                            aggr_result_list.append(aggr_result)
                        if not is_true and len(rid_list_1) == 0:
                            a_count = self.smm.Aggregator_without_where(tb_name_list[0], "*", Aggregator.COUNT)
                        else:
                            a_count = self.smm.Aggregator_with_where(tb_name_list[0], "*", Aggregator.COUNT, rid_list_1)
                        if not is_true and len(rid_list_2) == 0:
                            b_count = self.smm.Aggregator_without_where(tb_name_list[1], "*", Aggregator.COUNT)
                        else:
                            b_count = self.smm.Aggregator_with_where(tb_name_list[1], "*", Aggregator.COUNT, rid_list_2)
                        if a_count * b_count == 0:
                            for i in range(len(aggr_list)):
                                if aggr_list[i][0] == Aggregator.COUNT:
                                    aggr_result_list[i] = 0
                                else:
                                    aggr_result_list[i] = "NULL"
                        else:
                            for i, aggr in enumerate(aggr_list):
                                if aggr[1] == "*":
                                    aggr_result_list[i] = a_count * b_count
                                elif aggr[0] == Aggregator.COUNT or aggr[0] == Aggregator.SUM:
                                    if aggr_result_list[i] != "NULL":
                                        count_ = a_count if aggr[1][0] == tb_name_list[1] else b_count
                                        aggr_result_list[i] *= count_
                else:
                    #这时候的统计是有点不准的，把NULL都视为NULL，如果有一个字符串叫NULL就难受了
                    #而且类型报错也有点不准
                    attr_name, res = self.smm.Select_with_WhereClause_Join(tb_name_list, [], rid_list_1, rid_list_2, is_true, cross_clause_list)
                    for aggr in aggr_list:
                        if aggr[1] == "*":
                            aggr_result_list.append(len(res))
                        else:
                            count_ = 0
                            sum_ = 0
                            has = False
                            for i, attr in enumerate(attr_name):
                                if f"{aggr[1][0]}.{aggr[1][1]}" == attr:
                                    has = True
                                    for res_ in res:
                                        if res_[i] != "NULL":
                                            count_ += 1
                                            if isinstance(res_[i], str) and aggr[0] != Aggregator.COUNT:
                                                raise myErr("AggregatorError", f"The attr {aggr[1][1]} doesn't support the aggregator function.")
                                            if aggr[0] == Aggregator.SUM or aggr[0] == Aggregator.AVERAGE:
                                                sum_ += res_[i]
                                            elif aggr[0] == Aggregator.MAX:
                                                if count_ == 1:
                                                    sum_ = res_[i]
                                                else:
                                                    sum_ = max(res_[i], sum_)
                                            elif aggr[0] == Aggregator.MIN:
                                                if count_ == 1:
                                                    sum_ = res_[i]
                                                else:
                                                    sum_ = min(res_[i], sum_)
                                    break
                            if not has:
                                raise myErr("AttrError", "You gave a wrong attr name.")
                            if aggr[0] == Aggregator.COUNT:
                                aggr_result_list.append(count_)
                            elif aggr[0] == Aggregator.AVERAGE:
                                aggr_result_list.append("NULL" if count_ == 0 else sum_ / count_)
                            else:
                                aggr_result_list.append("NULL" if count_ == 0 else sum_)



                output_field = []
                output_result = []
                aggr_num = 0
                should_tips = False
                if group_name is not None:
                    output_field.append(group_name)
                for selector in select_list:
                    if selector[0] == Aggregator.NO:
                        output_field.append(f"{selector[1][0]}.{selector[1][1]}" if selector[1][0] != "" else selector[1][1])
                        output_result.append("-")
                        should_tips = True
                    else:
                        output_result.append(aggr_result_list[aggr_num])
                        if selector[0] == Aggregator.COUNT:
                            selector_text = "COUNT"
                        elif selector[0] == Aggregator.AVERAGE:
                            selector_text = "AVG"
                        elif selector[0] == Aggregator.MAX:
                            selector_text = "MAX"
                        elif selector[0] == Aggregator.MIN:
                            selector_text = "MIN"
                        else:
                            selector_text = "SUM"
                        if selector[1] == "*":
                            tbn = "*"
                        else:
                            tbn = f"{selector[1][0]}.{selector[1][1]}" if selector[1][0] != "" else selector[1][1]
                        output_field.append(f"{selector_text}({tbn})")
                        aggr_num += 1
                if group_name is not None:
                    return SQLOutput("", True, output_field, aggr_result_list)
                else:
                    follow_msg = "Due to there is(are) aggregator function(s), the other column(s) is(are) not showed." if should_tips else ""
                    return SQLOutput("", True, output_field, [output_result], True, follow_msg)
                
            #这种时候就是有万恶的cross_clause的，直接select了，不管性能了


    # Visit a parse tree produced by SQLParser#alter_add_index.
    def visitAlter_add_index(self, ctx:SQLParser.Alter_add_indexContext):
        tb_name = ctx.Identifier().getText()
        index_name = ctx.identifiers().getText()
        self.smm.CreateIndex(tb_name, index_name)
        return SQLOutput(f"Successfully created index {index_name} on {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#alter_drop_index.
    def visitAlter_drop_index(self, ctx:SQLParser.Alter_drop_indexContext):
        tb_name = ctx.Identifier().getText()
        index_name = ctx.identifiers().getText()
        self.smm.DropIndex(tb_name, index_name)
        return SQLOutput(f"Successfully dropped index {index_name} on {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#alter_table_drop_pk.
    def visitAlter_table_drop_pk(self, ctx:SQLParser.Alter_table_drop_pkContext):
        tb_name = ctx.Identifier()[0].getText()
        self.smm.DropPK(tb_name)
        return SQLOutput(f"Successfully dropped the primary key on {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#alter_table_drop_foreign_key.
    def visitAlter_table_drop_foreign_key(self, ctx:SQLParser.Alter_table_drop_foreign_keyContext):
        vice_table = ctx.Identifier(0).getText()
        fk_name =ctx.Identifier(1).getText()
        self.smm.DropFK(vice_table, fk_name)
        return SQLOutput(f"Successfully dropped the foreign key {fk_name}.", False)



    # Visit a parse tree produced by SQLParser#alter_table_add_pk.
    def visitAlter_table_add_pk(self, ctx:SQLParser.Alter_table_add_pkContext):
        tb_name = ctx.Identifier()[0].getText()
        pk_name = ctx.identifiers().getText()
        self.smm.AddPK(tb_name, pk_name)
        return SQLOutput(f"Successfully added primary key {pk_name} on {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#alter_table_add_foreign_key.
    def visitAlter_table_add_foreign_key(self, ctx:SQLParser.Alter_table_add_foreign_keyContext):
        vice_table = ctx.Identifier(0).getText()
        if ctx.Identifier(2) is None:
            fk_name = ""
            main_table = ctx.Identifier(1).getText()
        else:
            fk_name = ctx.Identifier(1).getText()
            main_table = ctx.Identifier(2).getText()
        vice_column = ctx.identifiers(0).getText()
        main_column = ctx.identifiers(1).getText()
        self.smm.AddFK(main_table, main_column, vice_table, vice_column, fk_name)
        return SQLOutput(f"Successfully added the foreign key.", False)


    # Visit a parse tree produced by SQLParser#alter_table_add_unique.
    def visitAlter_table_add_unique(self, ctx:SQLParser.Alter_table_add_uniqueContext):
        tb_name = ctx.Identifier().getText()
        unique_name = ctx.identifiers().getText()
        self.smm.AddUnique(tb_name, unique_name)
        return SQLOutput(f"Successfully added unique constraint on {unique_name} in {tb_name}.", False)


    # Visit a parse tree produced by SQLParser#field_list.
    def visitField_list(self, ctx:SQLParser.Field_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#normal_field.
    def visitNormal_field(self, ctx:SQLParser.Normal_fieldContext):
        typ = ctx.type_().getText()
        typ_ = ATTR_TYPE.INT
        length = 4
        if typ == "FLOAT":
            typ_ = ATTR_TYPE.FLOAT
        elif typ != "INT":
            typ_ = ATTR_TYPE.STRING
            length = int("".join(list(filter(str.isdigit, typ))))

        null = ctx.Null() is None
        if ctx.value() is None:
            value = None
            default_is_null = True
        else:
            value = ctx.value().getText()
            if value == "NULL":
                default_is_null = True
            else:
                default_is_null = False
                if typ_ == ATTR_TYPE.STRING:
                    if value[0] == "'" and value[-1] == "'":
                        value = value[1:-1]
                    else:
                        raise myErr("IllegalType", "Except a string type.")
                elif value[0] == "'" or value[-1] == "'":
                    raise myErr("IllegalType", "Don't want a string type.")
                elif typ_ == ATTR_TYPE.INT:
                    value = int(value)
                else:
                    value = float(value)

        return [ctx.Identifier().getText(), typ_, length, null, value, default_is_null]


    # Visit a parse tree produced by SQLParser#primary_key_field.
    def visitPrimary_key_field(self, ctx:SQLParser.Primary_key_fieldContext):
        return [ctx.identifiers().getText()]


    # Visit a parse tree produced by SQLParser#foreign_key_field.
    def visitForeign_key_field(self, ctx:SQLParser.Foreign_key_fieldContext):
        vice_column = ctx.identifiers(0).getText()
        main_column = ctx.identifiers(1).getText()
        if ctx.Identifier(1) is not None:
            fk_name = ctx.Identifier(0).getText()
            main_table = ctx.Identifier(1).getText()
        else:
            fk_name = ""
            main_table = ctx.Identifier(0).getText()

        return [fk_name, main_table, main_column, vice_column]


    # Visit a parse tree produced by SQLParser#type_.
    def visitType_(self, ctx:SQLParser.Type_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#value_lists.
    def visitValue_lists(self, ctx:SQLParser.Value_listsContext):
        i = 0
        l = []
        while ctx.value_list(i) is not None:
            l.append(self.visitValue_list(ctx.value_list(i)))
            i += 1
        return l


    # Visit a parse tree produced by SQLParser#value_list.
    def visitValue_list(self, ctx:SQLParser.Value_listContext):
        i = 0
        value_list = []
        type_list = []
        while True:
            if ctx.value(i) is None:
                return value_list, type_list
            s = ctx.value(i).getText()
            if s == "NULL":
                type_list.append(ATTR_TYPE.NONETYPE)
                value_list.append(s)
            elif s[0] == "'" and s[-1] == "'":
                type_list.append(ATTR_TYPE.STRING)
                value_list.append(s[1:-1])
            elif s.find(".") == -1:
                type_list.append(ATTR_TYPE.INT)
                value_list.append(int(s))
            else:
                type_list.append(ATTR_TYPE.FLOAT)
                value_list.append(float(s))
            i += 1


    # Visit a parse tree produced by SQLParser#value.
    def visitValue(self, ctx:SQLParser.ValueContext):
        return [ctx.getText()]


    # Visit a parse tree produced by SQLParser#where_and_clause.
    def visitWhere_and_clause(self, ctx:SQLParser.Where_and_clauseContext):
        # where_clause_list = self.visitChildren(ctx)
        # self.smm.Process_where_and_clause(where_clause_list)
        l = self.visitChildren(ctx)
        return l if isinstance(l[0], list) else [l]


    # Visit a parse tree produced by SQLParser#where_operator_expression.
    def visitWhere_operator_expression(self, ctx:SQLParser.Where_operator_expressionContext):
        operator = ctx.operator_().getText()
        if operator == "=":
            ope = WhereClause.EQ
        elif operator == "<":
            ope = WhereClause.LT
        elif operator == "<=":
            ope = WhereClause.LE
        elif operator == ">":
            ope = WhereClause.GT
        elif operator == ">=":
            ope = WhereClause.GE
        else:
            ope = WhereClause.NE
        return [ope, self.visitColumn(ctx.column()), self.visitExpression(ctx.expression())]


    # Visit a parse tree produced by SQLParser#where_operator_select.
    def visitWhere_operator_select(self, ctx:SQLParser.Where_operator_selectContext):
        raise myErr("NotSupport", "We don't support where operator select clause.")
        # return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_null.
    def visitWhere_null(self, ctx:SQLParser.Where_nullContext):
        if ctx.getText().endswith("NOTNULL"):
            ope = WhereClause.IS_NOT_NULL
        else:
            ope = WhereClause.IS_NULL
        return [ope, self.visitColumn(ctx.column())]


    # Visit a parse tree produced by SQLParser#where_in_list.
    def visitWhere_in_list(self, ctx:SQLParser.Where_in_listContext):
        vl, tl = self.visitValue_list(ctx.value_list())
        return [WhereClause.IN, self.visitColumn(ctx.column()), vl, tl]


    # Visit a parse tree produced by SQLParser#where_in_select.
    def visitWhere_in_select(self, ctx:SQLParser.Where_in_selectContext):
        raise myErr("NotSupport", "We don't support where in select clause.")
        # return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#where_like_string.
    def visitWhere_like_string(self, ctx:SQLParser.Where_like_stringContext):
        # raise myErr("NotSupport", "We don't support where like string clause.")
        return [WhereClause.LIKE, self.visitColumn(ctx.column()), ctx.String().getText()[1:-1]]


    # Visit a parse tree produced by SQLParser#column.
    def visitColumn(self, ctx:SQLParser.ColumnContext):
        l = []
        i = 0
        while ctx.Identifier(i) is not None:
            l.append(ctx.Identifier(i).getText())
            i += 1
        return l if len(l) > 1 else ["", l[0]]


    # Visit a parse tree produced by SQLParser#expression.
    def visitExpression(self, ctx:SQLParser.ExpressionContext):
        if ctx.column() is not None:
            return [False, self.visitColumn(ctx.column())]
        s = ctx.value().getText()
        if s == "NULL":
            raise myErr("NullError", "Expression can't be NULL.")
        elif len(s) != 0 and s[0] == "'" and s[-1] == "'":
            typ = ATTR_TYPE.STRING
            value = (s[1:-1])
        elif s.find(".") == -1:
            typ = ATTR_TYPE.INT
            value = (int(s))
        else:
            typ = ATTR_TYPE.FLOAT
            value = (float(s))
        return [True, value, typ]


    # Visit a parse tree produced by SQLParser#set_clause.
    def visitSet_clause(self, ctx:SQLParser.Set_clauseContext):
        set_clause_list = []
        i = 0
        column_name_pool = set()
        while ctx.Identifier(i) is not None:
            s = ctx.value(i).getText()
            if s == "NULL":
                value = [s, ATTR_TYPE.NONETYPE]
            elif len(s) != 0 and s[0] == "'" and s[-1] == "'":
                value = [s[1:-1], ATTR_TYPE.STRING]
            elif s.find(".") == -1:
                value = [int(s), ATTR_TYPE.INT]
            else:
                value = [float(s), ATTR_TYPE.FLOAT]
            set_clause_list.append([ctx.Identifier(i).getText(), value])
            column_name_pool.add(ctx.Identifier(i).getText())
            i += 1
        if len(column_name_pool) != len(set_clause_list):
            raise myErr("UpdateError", "You set value twice in one column.")
        return set_clause_list


    # Visit a parse tree produced by SQLParser#selectors.
    def visitSelectors(self, ctx:SQLParser.SelectorsContext):
        selector_list = self.visitChildren(ctx)
        if selector_list is None:
            return None
        if not isinstance(selector_list[0], list):
            selector_list = [selector_list]
        return selector_list

    # Visit a parse tree produced by SQLParser#selector.
    def visitSelector(self, ctx:SQLParser.SelectorContext):
        
        if ctx.Count() is not None:
            return [Aggregator.COUNT, "*"]
        elif ctx.aggregator() is not None:
            aggr_text = ctx.aggregator().getText()
            if aggr_text == "COUNT":
                aggr = Aggregator.COUNT
            elif aggr_text == "AVG":
                aggr = Aggregator.AVERAGE
            elif aggr_text == "MAX":
                aggr = Aggregator.MAX
            elif aggr_text == "MIN":
                aggr = Aggregator.MIN
            else:
                aggr = Aggregator.SUM
            return [aggr, self.visitColumn(ctx.column())]
        else:
            return [Aggregator.NO, self.visitColumn(ctx.column())]


    # Visit a parse tree produced by SQLParser#identifiers.
    def visitIdentifiers(self, ctx:SQLParser.IdentifiersContext):
        name_list = []
        i = 0
        while ctx.Identifier(i) is not None:
            name_list.append(ctx.Identifier(i).getText())
            i += 1
        return name_list


    # Visit a parse tree produced by SQLParser#operator_.
    def visitOperator_(self, ctx:SQLParser.Operator_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SQLParser#aggregator.
    def visitAggregator(self, ctx:SQLParser.AggregatorContext):
        return self.visitChildren(ctx)



del SQLParser