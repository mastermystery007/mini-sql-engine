import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Parenthesis,Function
from sqlparse.tokens import Keyword, DML, Wildcard
import csv


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
        #f = open(self.name+".csv", "r")
        #line = f.readline()
        with open(self.name+".csv", 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                self.table.append(row)

        #while line:
            
        #    list_my = line.strip('\""\n\r').split(',')
        #    self.table.append(list_my)
            """
            for i in range (len(self.cols_names)):
                #print("filling "+list_my[i]+" at "+self.cols_names[i])
                self.col_dict[self.cols_names[i]].append(list_my[i])
            """
        #    line = f.readline()

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
    orderbycolumn = 0

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
                    if token.value.upper() == "COUNT":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "MAX":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "MIN":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "DISTINCT":
                        self.add_token_to_list(token.value)
                    if token.value.upper() == "ORDER BY":
                        self.set_booleans()
                        self.orderbyboolean = True
                    

                if token.ttype is Wildcard:
                    self.add_token_to_list(token.value)

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
        if len(self.fromlist) == 0:
            print("Atleast one table must be selected")
            return 
        if len(self.selectlist) == 0:
            print("Atleast one column must be selected")
            return 

        #print("where list ")
        #print(self.wherelist)
        #print("group by list ")
        #print (self.groupbylist)
        #print("select list is ")
        #print(self.selectlist)
        #print("order by list ")
        #print(self.orderbylist)



        self.loadTables()  # from
        self.whereQuery()   #where
        self.groupbyQuery() #groupby
        self.selectQuery()  #select


        #print ("orderbylist")
        #print(self.orderbylist)
        #print(len(self.table.table))
        #print("col names ")
        #print(self.table.cols_names)
        
        self.orderbyQuery()
        self.show_sanitiized_table()

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

        if len(self.wherelist)==0:
            return
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
            
            

            newtable1 =  self.resolveWhereQuery(self.get_operator(self.wherelist[2]), p1, p2)
            newtable2 =  self.resolveWhereQuery(self.get_operator(self.wherelist[6]),p3, p4)

            for row in self.table.table:
                if row in newtable1 or row in newtable2:
                    newtable.append(row)

        self.table.table = newtable 


    def selectQuery(self):

        if "*" in self.selectlist:
            self.selectlist.remove("*")
            for name in self.table.cols_names:
                self.selectlist.append(name)

        if len(self.groupbylist)==0:
            for i in range (0,len(self.selectlist)):
                if self.selectlist[i]=="MAX":
                    a = i+1
                    c = self.selectlist[a]
                    self.domaxoncol(c)
                if self.selectlist[i]=="MIN":
                    a = i+1
                    c = self.selectlist[a]
                    self.dominoncol(c)
                if self.selectlist[i]=="AVG":
                    a = i+1
                    c = self.selectlist[a]
                    self.doavgoncol(c)
                if self.selectlist[i]=="SUM":
                    a = i+1
                    c = self.selectlist[a]
                    self.dosumoncol(c)
                if self.selectlist[i]=="COUNT":
                    a = i+1
                    c = self.selectlist[a] 
                    self.docountoncol(c)
            
            self.table.table = [self.table.table[0]]

        else:
            for i in range (0,len(self.selectlist)):
                if self.selectlist[i]=="MAX":
                    a = i+1
                    c = self.selectlist[a]
                    
                    self.domaxontable(c)
                if self.selectlist[i]=="MIN":
                    a = i+1
                    c = self.selectlist[a]
                    
                    self.dominontable(c)
                if self.selectlist[i]=="AVG":
                    a = i+1
                    c = self.selectlist[a]
                    
                    self.doavgontable(c)
                if self.selectlist[i]=="SUM":
                    a = i+1
                    c = self.selectlist[a]
                    
                    self.dosumontable(c)
                if self.selectlist[i]=="COUNT":
                    a = i+1
                    c = self.selectlist[a]
                    
                    self.docountontable(c)
        
        self.convertlisttoval()
        
    def domaxoncol(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        r=[]
        for row in self.table.table:
            r.append(int(row[col_num])) 
        self.table.table[0][col_num]=max(r)
    
    def dosumoncol(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        r=[]
        for row in self.table.table:
            r.append(int(row[col_num])) 
        print(r)
        self.table.table[0][col_num]=sum(r)
    
    def dominoncol(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        r=[]
        for row in self.table.table:
            r.append(int(row[col_num])) 
        self.table.table[0][col_num]=min(r)

    def doavgoncol(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        r=[]
        for row in self.table.table:
            r.append(int(row[col_num])) 
        self.table.table[0][col_num]= sum(r) / len(r) 

    def docountoncol(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        r=[]
        for row in self.table.table:
            r.append(int(row[col_num])) 
        self.table.table[0][col_num]=  len(r)

    def docountontable(self,col_name):
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name ==self.table.cols_names[i]:
                col_num = i
                break
        for row in self.table.table:
            row[col_num] = len(row[col_num])  

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

        if len(self.groupbylist)==0:
            return
        table_3=[]
        col_num = 0
        for i in range (0,len(self.table.cols_names)):
            if self.groupbylist[0] == self.table.cols_names[i]:
                col_num=i
                break
        
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
        

    def convertlisttoval(self):
        for row in self.table.table:
            for i in range (0,len(row)):
                if type(row[i])==list and len(row[i])==1:
                    row[i] = row[i][0]

     


    def show_sanitiized_table(self): 
        flag = False
        distinct = 0
        

        for i in range (0,len(self.selectlist)):
            
            if self.selectlist[i] == "MAX":
                flag=True
                self.show_columns.append("max("+self.selectlist[i+1]+")")
                #i=i+1
            elif self.selectlist[i] == "MIN":
                flag=True
                self.show_columns.append("min("+self.selectlist[i+1]+")")
                #i=i+1
            elif self.selectlist[i] == "AVG":
                flag=True
                self.show_columns.append("avg("+self.selectlist[i+1]+")")
                #i=i+1
            elif self.selectlist[i] == "SUM":
                flag=True
                self.show_columns.append("sum("+self.selectlist[i+1]+")")
                #i=i+1
            elif self.selectlist[i] == "COUNT":
                flag=True
                self.show_columns.append("count("+self.selectlist[i+1]+")")
                #i=i+1
            elif self.selectlist[i] == "DISTINCT":
                distinct=1
                
            else:
                if flag:
                    flag=0
                else:
                    self.show_columns.append(self.selectlist[i])
                
                
        print ("show_columns")
        print(self.show_columns)
        showcols=[]
        for i in range (0,len(self.selectlist)):
            if self.selectlist[i] in self.table.cols_names :
                for j in range (0,len(self.table.cols_names)):
                    if self.selectlist[i] == self.table.cols_names[j]:
                        showcols.append(j)
        

        #print(self.show_columns)
        finaltable = []
        tempo = []
        for row in self.table.table:
            col = []
            for index in showcols:
                col.append(row[index])
                #print(row[index],end = "           ")
            finaltable.append(col)
            #print(" ")
        
        if distinct==1:
            for row in finaltable:
                if row not in tempo:
                    print(row)
                tempo.append(row)
        else:
            for row in finaltable:
                print(row)
                


        
        
    def getorderbycolumn(self,col_name):
        ordervy = 0
        for i in range (0,len(self.table.cols_names)):
            if col_name == self.table.cols_names[i]:
                ordervy = i
                self.orderbycolumn=i
                print("order by col is "+str(i))
                return 
        
    def orderbyQuery(self):
        if len(self.orderbylist)==0:
            return 

        self.getorderbycolumn(self.orderbylist[0])
        self.table.table.sort(key=self.takeSome)
        #self.table.table.sort(key=lambda x:x.split()[ordervy]
        """
        for row in self.table.table:
            print(row)
        """
        #
    def takeSome(self,elem):
        #print(elem)
        return elem[self.orderbycolumn]
        
    def resolveWhereQuery(self, operator_type, col1, col2):
        tablefinal = []
        if type(col1) == str and type(col2) == int:

            col_num = 0
            for i in range(0, len(self.table.cols_names)):
                if self.table.cols_names[i] == col1:
                    col_num = i
                    break
            
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



abc = "Select SUM(A),MAX(E) FROM table1,table2 GROUP BY B;"
mydb = Database("metadata.txt")
queryt = Query(abc, mydb)

queryt.parse_query()
queryt.executQuery()


#db=Database("metadata.txt")
