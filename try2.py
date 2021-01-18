import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Parenthesis,Function
from sqlparse.tokens import Keyword, DML, Wildcard


class Database:
    def __init__(self, location):
        self.location = location
        self.table_names = []  # all the table names
        self.tables = {}  # dictionary of table objects, accessed by Tnames
        self.add_all_tables()

    def add_all_tables(self):
        f = open(self.location, "r")
        line = f.readline()  # begin_table
        while line:
            line = line.split()[0]
            if line == '<begin_table>':
                cols = []  # all column names will be stores here
                line = f.readline()
                if not line or line == '<end_table>':
                    return -1
                line = line.split()[0]
                Tname = line  # Add table name to list here
                line = f.readline()
                if not line:
                    return -1
                while line and line.split()[0] != '<end_table>':

                    cols.append(line.split()[0])
                    line = f.readline()

                self.table_names.append(Tname)
                self.tables[Tname] = Table(Tname, cols)

            else:
                return -1
            line = f.readline()


class Table():
    def __init__(self, name, cols):
        #print(name)
        self.name = name
        self.cols_names = cols
        self.table = []
        #self.col_dict = {}
        #self.init_col_dicts()
        self.fill_table()
        #self.print_rows()


    def fill_table(self):
        f = open(self.name+".txt", "r")
        line = f.readline()

        while line:
            list_my = line.strip('\n').split(',')
            self.table.append(list_my)
            """
            for i in range (len(self.cols_names)):
                #print("filling "+list_my[i]+" at "+self.cols_names[i])
                self.col_dict[self.cols_names[i]].append(list_my[i])
            """
            line = f.readline()

    def print_rows(self):
        for row in self.table:
            print(row)


class Query():
    query = ""
    table = None
    dbase = None
    selectboolean = False
    fromboolean = False
    whereboolean = False
    groupbyboolean = False
    havingboolean = False
    orderbyboolean = False
    selectlist = []
    fromlist = []
    wherelist = []
    groupbylist = []
    orderbylist = []
    havinglist = []
    show_columns = []

    def __init__(self, query, db):
        self.dbase = db
        self.query = query

    def set_booleans(self):
            self.selectboolean = False
            self.fromboolean = False
            self.whereboolean = False
            self.groupbyboolean = False
            self.havingboolean = False
            self.orderbyboolean = False

    def add_token_to_list(self, sentence):
        sentence = sentence.replace(',', ' ')
        sentence = sentence.replace('(', ' ')
        sentence = sentence.replace(')', ' ')
        s = sentence.split(' ')
        for word in s:
            if word == ' ' or word == '':
                continue
            if self.selectboolean:
                self.selectlist.append(word)

            if self.fromboolean:
                self.fromlist.append(word)

            if self.groupbyboolean:
                self.groupbylist.append(word)

            if self.havingboolean:
                self.havinglist.append(word)

            if self.orderbyboolean:
                self.orderbylist.append(word)

            if self.whereboolean:
                self.wherelist.append(word)

    def parse_query(self):
            t = self.query
            q = sqlparse.format(t, keyword_case='upper')
            elements = sqlparse.parse(q)[0].tokens

            for token in elements:
                if token.ttype is Keyword:
                    if token.value.upper() == "FROM":
                        self.set_booleans()
                        self.fromboolean = True
                    if token.value.upper() == "GROUP BY":
                        self.set_booleans()
                        self.groupbyboolean = True
                    if token.value.upper() == "AVG":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "MAX":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "MIN":
                        self.add_token_to_list(token.value)
                    

                #if token.ttype is Wildcard:

                if token.ttype is DML:
                    if token.value.upper() == "SELECT":
                        self.set_booleans()
                        self.selectboolean = True

                if isinstance(token, IdentifierList):
                    self.add_token_to_list(token.value)

                if isinstance(token, Identifier):
                    self.add_token_to_list(token.value)
                
                if isinstance(token, Function):
                    self.add_token_to_list(token.value)

                if isinstance(token, Where):

                    self.set_booleans()
                    self.whereboolean = True
                    self.add_token_to_list(token.value)
                    self.set_booleans()

                if isinstance(token, Parenthesis):
                    temp1 = token.value.replace(')', '')
                    temp2 = temp1.replace('(', '')
                    self.add_token_to_list(temp2)



    def executQuery(self):
        self.loadTables()  # from
        self.whereQuery()
        self.groupbyQuery()
        self.selectQuery()
        for row in self.table.table:
            print (row)

        """
        
        self.orderQuery()
        
        """

    def loadTables(self):
        for table in self.dbase.table_names:
            if table in self.fromlist:
                if self.table == None:
                    self.table = self.dbase.tables[table]
                    self.table.name = "query_table"

                else:
                    table_temperary = self.dbase.tables[table]
                    for cols in table_temperary.cols_names:
                        self.table.cols_names.append(cols)

                    t = []
                    for my_row in self.table.table:
                        for temp_row in table_temperary.table:
                            t1 = my_row+temp_row
                            t.append(t1)
                    self.table.table = t
    
    
    def is_column(self,x):
        
        if x in self.table.cols_names:
            return True
        if re.search("^\d+$", x):
            return False
        return False

    def get_operator(self,operator):
        if ">" == operator:
            operator_type = 1

        if ">=" == operator:
            operator_type = 2

        if "<" == operator:
            operator_type = 3

        if "<=" == operator:
            operator_type = 4

        if "=" == operator:
            operator_type = 0

        return operator_type
    
    def whereQuery(self):
        print(self.wherelist)
        print(self.table.cols_names)
        if "AND" in self.wherelist:
            and_or_present = 1
        elif "OR" in self.wherelist:
            and_or_present = 2
        else:
            and_or_present = 3

        if ">" in self.wherelist:
            operator_type = 1
        if ">=" in self.wherelist:
            operator_type = 2
        if "<" in self.wherelist:
            operator_type = 3
        if "<=" in self.wherelist:
            operator_type = 4
        if "=" in self.wherelist:
            operator_type = 0
        newtable1 = []
        newtable2 = []
        newtable = []

        if and_or_present == 3:
            if self.wherelist[1] in self.table.cols_names:
                if self.wherelist[3] in self.table.cols_names:
                    newtable = self.resolveWhereQuery(
                        operator_type, self.wherelist[1], self.wherelist[3])
                else:
                    print("the operator type is "+str(operator_type))
                    newtable = self.resolveWhereQuery(
                        operator_type, self.wherelist[1], int(self.wherelist[3]))
                     
            else:
                newtable = self.resolveWhereQuery(
                    operator_type, int(self.wherelist[1]), self.wherelist[3])

        if and_or_present == 1:
            if self.is_column(self.wherelist[1]):
                p1=self.wherelist[1]
            else:
                p1=int(self.wherelist[1])

            if self.is_column(self.wherelist[3]):
                p2=self.wherelist[3]
            else:
                p2=int(self.wherelist[3])
            
            if self.is_column(self.wherelist[5]):
                p3=self.wherelist[5]
            else:
                p3=int(self.wherelist[5])
            
            if self.is_column(self.wherelist[7]):
                p4=self.wherelist[7]
            else:
                p4=int(self.wherelist[7])
            
            print(self.get_operator(self.wherelist[2]))
            print(self.get_operator(self.wherelist[6]))

            newtable1 =  self.resolveWhereQuery(self.get_operator(self.wherelist[2]), p1, p2)
            newtable2 =  self.resolveWhereQuery(self.get_operator(self.wherelist[6]),p3, p4)

            for row in self.table.table:
                if row in newtable1 and row in newtable2:
                    newtable.append(row)
        



        if and_or_present == 2:
            if self.is_column(self.wherelist[1]):
                p1=self.wherelist[1]
            else:
                p1=int(self.wherelist[1])

            if self.is_column(self.wherelist[3]):
                p2=self.wherelist[3]
            else:
                p2=int(self.wherelist[3])
            
            if self.is_column(self.wherelist[5]):
                p3=self.wherelist[5]
            else:
                p3=int(self.wherelist[5])
            
            if self.is_column(self.wherelist[7]):
                p4=self.wherelist[7]
            else:
                p4=int(self.wherelist[7])
            
            print(self.get_operator(self.wherelist[2]))
            print(self.get_operator(self.wherelist[6]))

            newtable1 =  self.resolveWhereQuery(self.get_operator(self.wherelist[2]), p1, p2)
            newtable2 =  self.resolveWhereQuery(self.get_operator(self.wherelist[6]),p3, p4)

            for row in self.table.table:
                if row in newtable1 or row in newtable2:
                    newtable.append(row)

        self.table.table = newtable 


    def selectQuery(self):
        print(self.selectlist)
        for i in range (0,len(self.selectlist)):
            if self.selectlist[i]=="MAX":
                a = i+1
                c = self.selectlist[a]
                print("select list col is "+self.selectlist[a])
                self.domaxontable(c)
            if self.selectlist[i]=="MIN":
                a = i+1
                c = self.selectlist[a]
                print("select list col is"+self.selectlist[a])
                self.dominontable(c)
            if self.selectlist[i]=="AVG":
                a = i+1
                c = self.selectlist[a]
                print("select list col is"+self.selectlist[a])
                self.doavgontable(c)
            if self.selectlist[i]=="SUM":
                a = i+1
                c = self.selectlist[a]
                print("select list col is"+self.selectlist[a])
                self.dosumontable(c)

    def domaxontable(self , col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        for row in self.table.table:
            row[col_num] = max(row[col_num])

    def dominontable(self , col_name):
        col_num = -1
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        for row in self.table.table:
            row[col_num] = min(row[col_num])

    def doavgontable(self,col_name):
         
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        for row in self.table.table:
            row[col_num] = sum(row[col_num]) / len(row[col_num]) 

    def dosumontable(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        for row in self.table.table:
            row[col_num] = sum(row[col_num])

    def groupbyQuery(self):
        print (self.groupbylist)
        table_3=[]
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if self.groupbylist[0] == self.table.cols_names[i]:
                col_num=i
        print ("col_num is "+str(col_num))
        for i in range (0,len(self.table.table)):
            row1 = self.table.table[i]
            gbv = row1[col_num]
            ispresent = False
            for j in range (0,len(table_3)):
                row2 = table_3[j]
                if  int(gbv) in row2[col_num] :
                    
                    for p in range (0,len(row1)):
                        if p==col_num:
                            continue
                        else:
                            table_3[j][p].append(int(row1[p]))

                        
                            
                    ispresent = True
                    break

            if not ispresent:
                finallist =[]
                for q in range (0,len(row1)):
                        finallist.append([int(row1[q])])
                table_3.append(finallist)

        self.table.table = table_3
                
            
        
    def resolveWhereQuery(self, operator_type, col1, col2):
        tablefinal = []
        if type(col1) == str and type(col2) == int:

            col_num = 0
            for i in range(0, len(self.table.cols_names)):
                if self.table.cols_names[i] == col1:
                    col_num = i
                    break
            print("col num is ")
            print(col_num)
            if operator_type == 2:
                for row in self.table.table:
                    if int(row[col_num]) >= col2:
                        tablefinal.append(row)

            if operator_type == 3:
                for row in self.table.table:
                    if int(row[col_num]) < col2:
                        tablefinal.append(row)

            if operator_type == 4:
                for row in self.table.table:
                    if int(row[col_num]) <= col2:
                        tablefinal.append(row)

            if operator_type == 0:
                for row in self.table.table:
                    if int(row[col_num]) == col2:
                        tablefinal.append(row)

            if operator_type == 1:
                for row in self.table.table:
                    if int(row[col_num]) > col2:
                        tablefinal.append(row)

        if type(col2) == str and type(col1) == int:

            for i in range(0, len(self.table.cols_names)):
                if self.table.cols_names[i] == col2:
                    col_num = i

            if operator_type == 2:
                for row in self.table.table:
                    if col1 >= int(row[col_num]):
                        tablefinal.append(row)

            if operator_type == 3:
                for row in self.table.table:
                    if col1 < int(row[col_num]):
                        tablefinal.append(row)

            if operator_type == 4:
                for row in self.table.table:
                    if col1 <= int(row[col_num]):
                        tablefinal.append(row)

            if operator_type == 0:
                for row in self.table.table:
                    if col1 == int(row[col_num]):
                        tablefinal.append(row)

            if operator_type == 1:
                for row in self.table.table:
                    if col1 > int(row[col_num]):
                        tablefinal.append(row)

        if type(col2) == str and type(col1) == str:
            cols = []
            for i in range(0, len(self.table.cols_names)):
                if self.table.cols_names[i] == col2 or self.table.cols_names[i] == col1:
                    cols.append(i)
            column1 = cols[0]
            column2 = cols[1]

            if operator_type == 2:
                for row in self.table.table:
                    if int(row[column1]) >= int(row[column2]):
                        tablefinal.append(row)

            if operator_type == 3:
                for row in self.table.table:
                    if int(row[column1]) < int(row[column2]):
                        tablefinal.append(row)

            if operator_type == 4:
                for row in self.table.table:
                    if int(row[column1]) <= int(row[column2]):
                        tablefinal.append(row)

            if operator_type == 0:
                for row in self.table.table:
                    if int(row[column1]) == int(row[column2]):
                        tablefinal.append(row)

            if operator_type == 1:
                for row in self.table.table:
                    if int(row[column1]) > int(row[column2]):
                        tablefinal.append(row)

        return tablefinal



abc = "Select AVG (D),AVG (C)    from table1 , table2 WHERE A > 5 GROUP BY B;"
mydb = Database("metadata.txt")
queryt = Query(abc, mydb)
queryt.parse_query()
queryt.executQuery()

#db=Database("metadata.txt")
