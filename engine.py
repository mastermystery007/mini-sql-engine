import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis, Identifier


class RecursiveTokenParser(object):
 def __init__(self, query):
    self.query = query
    self.names = []

 def get_table_names(self):
    elements = sqlparse.parse(self.query)

    for token in elements[0].tokens:

        if isinstance(token, Identifier):
            print("identifier",end=" ")
            print(token.value)
            self.identifier(token)
        elif isinstance(token, Parenthesis):
            print("paran",end=" ")
            print(token.value)
            self.parenthesis(token)

        elif isinstance(token, Where):
            print("where",end=" ")
            print(token.value)
            self.where(token)

    return [str(name).upper() for name in self.names]

 def where(self, token):

    for subtoken in token.tokens:
        if isinstance(subtoken, Comparison):
            print("compare",end=" ")
            print(subtoken.value)
            self.comparison(subtoken)

 def comparison(self, token):
    for subtoken in token.tokens:
        if isinstance(subtoken, Parenthesis):
            print("paran",end=" ")
            print(subtoken.value)
            self.parenthesis(subtoken)

 def parenthesis(self, token):

    for subtoken in token.tokens:
        if isinstance(subtoken, Identifier):
            print("identifier",end=" ")
            print(subtoken.value)
            self.identifier(subtoken)
        elif isinstance(subtoken, Parenthesis):
            print("paran",end=" ")
            print(subtoken.value)
            self.parenthesis(subtoken)

 def identifier(self, token):
    self.names.append(token)

 def get_query(self):  #
    return self.query


sql2 = "SELECT * FROM CITY WHERE Population = (SELECT * FROM (SELECT * FROM  City GROUP BY city_name))"
t = RecursiveTokenParser(sql2)

print(t.get_query())
print(t.get_table_names())
