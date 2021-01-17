import sys

import sqlparse
from sqlparse.sql import IdentifierList, Identifier,Where, Comparison, Parenthesis
from sqlparse.tokens import Keyword,DML,Wildcard

import re

aggregates = {"sum":1,"avg":2,"max":3,"min":4}
operators = {"<":1, ">":2, "<=":3, ">=":4, "=":5}
class SqlQuery():
    query=""
    fromdict=[]
    selectdict=[]
    wheredict=[]
    groupbydict=[]
    havingdict=[]

class DB():
    tables=[]
    
    
"""
class Table():
    cols=[]
    col_dict={}
    def __init__(self,name):
        self.name = name

    def fillTable(self):
        f = open(self.name+".csv","r")
        line = f.readline().split()[0]
        if line == '<begin_table>':
            a = f.readline().split()[0]
        print(a)
        while not a =='<end_table>':
            print(a)
            a=f.readline().split()[0]


    def load_tables(metadata_file,db):
        f = open(metadata_file,"r")
        line = f.readline()
        print(line)
"""




def fill_tokens(parsedQuery):
    for token in parsedQuery:
        if token.ttype is Keyword:
            print("keyword",end=" ")
            print(token.value)
        if token.ttype is Wildcard:
            print("wild",end=" ")
            print(token.value)    

        if isinstance(token, Identifier):
            print("identifier",end=" ")
            print(token.value)
        
        if isinstance(token, IdentifierList):
            print("identifierlist",end=" ")
            print(token.value)
        
        if token.ttype is DML:
            print("DML",end=" ")
            print(token.value)

        if  isinstance(token, Parenthesis):
            print(token.value)
            
            
        

def load_tables(metadata_file):
	f = open(metadata_file,"r")
	line = f.readline()
	while line:
		line = line.split()[0]
		if line == '<begin_table>':
			cols = []
			line = f.readline()
			if not line or line == '<end_table>':
				return -1
			line = line.split()[0]
			Tname = line
			line = f.readline()
			if not line:
				return -1
			while line and line.split()[0] != '<end_table>':
				print(line)
				cols.append(line.split()[0])
				line = f.readline()
			if not line:
				return -1
			#db.add_table_name(Tname)
			#db.add_table(Tname,Table(Tname,cols))
		else:
			return -1
		line = f.readline()
	return 0


query = "Select col1 ,MAX( col2 ) from table_n WHERE c1 > c2 AND c3 > c4;"
q = sqlparse.format(query,keyword_case='upper')
elements = sqlparse.parse(q)[0].tokens
fill_tokens(elements)

