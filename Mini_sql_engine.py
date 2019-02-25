#!/usr/bin/env python
# coding: utf-8

# In[1]:

# --- Aditya Tewari
# ---- 2018201082

# un comment the Commented print to Debug!


from __future__ import with_statement
import sqlparse
from sqlparse.sql import Where
import re
import sys
import csv
import itertools
import operator as op
import copy
from pprint import pprint
from collections import OrderedDict
# Definition for global variables
identifiers = []
tables = OrderedDict()
meta = OrderedDict()

operands = []
operator = []

aggregate = []
distinct = False
agg = False
a_func = ['sum','avg','max','min']
column_names = []
output = []

operator_list = {
    "<=": op.le,
    ">=": op.ge,
    "=": op.eq,
    ">": op.gt,
    "<": op.lt
}   
where = []

sql = sys.argv[1]
# sql = "select * from table1,table2  where  table1.A >=  1 and  table1.C >  table1.B;"
sql = re.sub(' +', ' ', sql)
# sql
print("")

def Error_Parsing(msg):
    print(msg)
    print("...Incorrect Format;")
    print("...Engine Abort");
    sys.exit(0)

def parser(sql):
    try :
        global identifiers
        query = sqlparse.parse(sql)[0].tokens
        
        qtype = sqlparse.sql.Statement(query).get_type()
        idlist = sqlparse.sql.IdentifierList(query).get_identifiers()
        for i in idlist:
            identifiers.append(str(i))
        if(qtype == 'SELECT' ):
            identifiers = identifiers[1:]
#             execute()
            pass
        else:
            Error_Parsing('Please Input Select Query only')
            
        
    except : 
        print("Except Block...Error Parsing")


def MetaData():
    m = "metadata.txt"
    try:
        with open(m,'r') as File:
            tableName = ""
            f = False
            for i in File:
                if i == "<begin_table>\n":
                    f = True
                    continue
                if i == "<end_table>\n":
                    f = False
                    continue
                elif(i == '<end_table>') :
                    continue
                else:
                    if(f == True):
                        i = i.rstrip()
                        tableName = i
                        tables[tableName]=OrderedDict()
                        meta[tableName] = []
                        f = False
                    else:
                        i = i.rstrip()
                        tables[tableName][(tableName+'.'+i)] = []  
                        meta[tableName].append(i)
    except:
        Error_Parsing("MetaData File Error!")

def ReadTable():
    try:
        global tables
        global meta
        for tableName in tables.keys():
            with open(tableName+".csv") as tab: 
                # print(tableName)
                data = csv.reader(tab)
                for column in data :
                    for k,t in zip(tables[tableName].keys(),column):
                        tables[tableName][k].append(int(t))
    except:
        Error_Parsing('File Error, Reading Tables...')

def table_name_check(table_names):
    global meta
    check = list(meta.keys())
    for i in table_names:
            if i not in check:
                Error_Parsing('Hmm, the table name seems incorrect...')
                
                
def column_name_check(table_names):
    global meta
    global column_names
    
    values = list(meta.values())
    if len(values) > 1:
        common = values[0]
        values = values[1:]
        for i in values:
            common = list(set(common).intersection(i))
#         print(common)
        if len(list(set(common).intersection(column_names))) !=0:
            Error_Parsing('Hmm, Ambigous column name')
    values = []
    redundant = list(meta.keys())
    dot = []
    for i in redundant:
        for j in meta[i]:
            values.append(j)
            dot.append(i+"."+j)
    for i in column_names:
        f = False
        if (i in dot) or (i in values):
            continue
        else:
            # print(column_names)
            Error_Parsing('Hmm, Some Column names are not correct, Wonder who typed wrong?')
    l = []
    for i in column_names:
        if(re.search('\.',i)):
            l.append(i)
        else:
            for x in redundant:
                for j in meta[x]:
                    if i == j:
                        l.append(x+'.'+j)
                
    column_names[:] = []
    column_names = copy.deepcopy(l)

def Join_Table(table_names):
#     table_names = table_names.split(',')
    temp = []
    for table in table_names:
        temp.append(tables[table])
    columns = []
    for dicts in temp:
        columns.append(zip(*dicts.values()))

    col_groups = []
    for c in itertools.product(*columns):
         col_groups.append(list(itertools.chain(*c)))

    rows = zip(*col_groups)
    combined_keys = list(itertools.chain(*temp))
    d_combined = OrderedDict((zip(combined_keys, rows)))
    return d_combined
#     print((list(d_combined.values())))

def column_name_process(table_names):
    global column_names
    global aggregate
    col_list = []
    temp = copy.deepcopy(column_names)
    k = 0
    f = False
#     print(column_names)
    for i in range(len(column_names)):
         for j in a_func:
            x = '^'+j+'(.*)$'
            if(re.search(x,column_names[i],re.IGNORECASE)):
                aggregate.append(j)
                col_list.append(column_names[i][len(j) + 1 : -1])
                k += 1
                f = True
    if f==True and k == len(column_names):
        column_names = col_list
        return (True)
    if f==False:
#         print(':::',temp)
        column_names = copy.deepcopy(temp)
#         print(':::',column_names)
        return (False)
    Error_Parsing("Hmm, Something is not right...")

def check_star():
    global column_names
    k=False
    if any("*" in s for s in column_names):
        if '*' in column_names and len(column_names)==1:
            k=True
        elif '*' not in column_names:
            k=True
        else:
            Error_Parsing('Hmmm,*s will Guide you Home')
    return k
        

def Check_Operands():
    global meta
    global operands
    
    table_names = list(meta.keys())
    values = list(meta.values())
    
    if len(values) > 1:
        common = values[0]
        values = values[1:]
        for i in values:
            common = list(set(common).intersection(i))
#         print(common)
        if len(list(set(common).intersection(operands))) !=0:
            Error_Parsing('Hmm, Ambigous column name')
    values = []
    redundant = list(meta.keys())
    dot = []
    for i in redundant:
        for j in meta[i]:
            values.append(j)
            dot.append(i+"."+j)
    for i in operands:
        f = False
        if (i in dot) or (i in values):
            continue
        if i.lstrip('-').isdigit():
            continue
        else:
            # print('^^',operands)
            Error_Parsing('--Hmm, Some Column names are not correct, Wonder who typed wrong?')
    l = []
    for i in operands:
        if(re.search('\.',i)):
            l.append(i)
        if i.lstrip('-').isdigit():
            l.append(i)
            continue
        else:
            for x in redundant:
                for j in meta[x]:
                    if i == j:
                        l.append(x+'.'+j)
#     print(':::',l)
    operands[:] = []
    operands = copy.deepcopy(l)

def Process_Where(condition):
    global tables
    global where
    global operator
    global operands
    for i in range(len(where)):
        # print(where)
        try:
            x = re.search("<=|>=|<|>|=",where[i])
            y = x.span()
            operands.append(where[i][0:y[0]].strip())
            operator.append(where[i][y[0]:y[1]].strip())
            operands.append(where[i][y[1]:].strip())
        
    #     w.strip() for w in operands
        
        # print(operands)
        # print(operator)
        # print('---',where)
        except:
            Error_Parsing('Hmm, Error in Where clause')
    Check_Operands()
    if condition==2 or condition ==1 :
        if len(operands)!=4 or len(operator)!=2:
            Error_Parsing('Hmm, error in WHERE condition')
    else :
        if len(operands)!=2 and len(operator)!=1:
            Error_Parsing('Hmm, error in WHERE ')
    
    new_table = []
    meta_list = list(tables.keys())
    tbd = []
    for i in range(len(tables[meta_list[0]])):
        l = []
        for k,v in tables.items():
            l.append(v[i])
        new_table.append(l)
    
    if condition == 0:
        op = operator_list[operator[0]]
        # print(new_table)
        for i in range(len(new_table)):
            if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                if op(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                    tbd.append(new_table[i])
                    
            elif operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == True:
                if op(new_table[i][meta_list.index(operands[0])],int(operands[1]))==False:
                    tbd.append(new_table[i])
            elif operands[0].lstrip('-').isdigit() == True and operands[1].lstrip('-').isdigit() == False:
                if op(int(operands[0],new_table[i][meta_list.index(operands[1])]))==False:
                    tbd.append(new_table[i])
                    
    elif condition == 1:
        op1 = operator_list[operator[0]]
        op2 = operator_list[operator[1]]
        for i in range(len(new_table)):
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==True:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])
                        elif op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            tbd.append(new_table[i])
                    
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == True and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==True:
                            if op2(int(operands[2],new_table[i][meta_list.index(operands[3])]))==False:
                                tbd.append(new_table[i])
                        elif op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            tbd.append(new_table[i])
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == True:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==True:
                            if op2(new_table[i][meta_list.index(operands[2])],int(operands[3]))==False:
                                tbd.append(new_table[i])
                        elif op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            tbd.append(new_table[i])
                if operands[0].lstrip('-').isdigit() == True and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(int(operands[0],new_table[i][meta_list.index(operands[1])]))==True:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])
                        elif op1(int(operands[0]),new_table[i][meta_list.index(operands[1])])==False:
                            tbd.append(new_table[i])
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == True:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==True:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])
                        elif op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==False:
                            tbd.append(new_table[i])
                if operands[0].lstrip('-').isdigit() == True and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == True and operands[3].lstrip('-').isdigit() == False:
                        if op1(int(operands[0],new_table[i][meta_list.index(operands[1])]))==True:
                            if op2(int(operands[2],new_table[i][meta_list.index(operands[3])]))==False:
                                tbd.append(new_table[i])
                        elif op1(int(operands[0]),new_table[i][meta_list.index(operands[1])])==False:
                            tbd.append(new_table[i])
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == True:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == True:
                        if op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==True:
                            if op2(new_table[i][meta_list.index(operands[2])],int(operands[3]))==False:
                                tbd.append(new_table[i])
                        elif op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==False:
                            tbd.append(new_table[i])
    elif condition == 2:
        op1 = operator_list[operator[0]]
        op2 = operator_list[operator[1]]
        for i in range(len(new_table)):
                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])


                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == True and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            if op2(int(operands[2],new_table[i][meta_list.index(operands[3])]))==False:
                                tbd.append(new_table[i])

                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == True:
                        if op1(new_table[i][meta_list.index(operands[0])],new_table[i][meta_list.index(operands[1])])==False:
                            if op2(new_table[i][meta_list.index(operands[2])],int(operands[3]))==False:
                                tbd.append(new_table[i])

                if operands[0].lstrip('-').isdigit() == True and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(int(operands[0],new_table[i][meta_list.index(operands[1])]))==False:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])

                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == True:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == False:
                        if op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==False:
                            if op2(new_table[i][meta_list.index(operands[2])],new_table[i][meta_list.index(operands[3])])==False:
                                tbd.append(new_table[i])

                if operands[0].lstrip('-').isdigit() == True and operands[1].lstrip('-').isdigit() == False:
                    if operands[2].lstrip('-').isdigit() == True and operands[3].lstrip('-').isdigit() == False:
                        if op1(int(operands[0],new_table[i][meta_list.index(operands[1])]))==False:
                            if op2(int(operands[2],new_table[i][meta_list.index(operands[3])]))==False:
                                tbd.append(new_table[i])

                if operands[0].lstrip('-').isdigit() == False and operands[1].lstrip('-').isdigit() == True:
                    if operands[2].lstrip('-').isdigit() == False and operands[3].lstrip('-').isdigit() == True:
                        if op1(new_table[i][meta_list.index(operands[0])],int(operands[1]))==False:
                            if op2(new_table[i][meta_list.index(operands[2])],int(operands[3]))==False:
                                tbd.append(new_table[i])
#     print('----',tbd)
    x = [item for item in new_table if item not in tbd]
    new_dict = OrderedDict()
    for i in meta_list:
        l = []
        for j in x:
            l.append(j[meta_list.index(i)])
        new_dict[i] = l
    return new_dict
#     print('list',new_dict)

def Operator_Processing(new_table,mlist,op,l_operand,cond):
    if l_operand[0].lstrip('-').isdigit() and l_operand[1].lstrip('-').isdigit():
        Error_Parsing('Hmm, Operands in where not correct!')
    
    

def Project_Table():
    global output
    global column_names
    global tables
    global distinct
    global agg
    meta = (list(tables.keys()))
#     print(column_names)
    new_table = []
    if agg == False:
        if '*' in column_names:
            column_names[:]=[]
            column_names = copy.deepcopy(meta)
        red = list(set(meta)-set(column_names))

        if len(output[0]) == 0:
            lazy = 0
            for i in column_names:
                if lazy==0:
                    print(i,sep='', end='')
                    lazy = 1
                else :
                    print(','+i,sep='', end='')
            print(' ')
            return []
        try:
	        for i in range(len(output)):
	            for x in red:
	                # print(output)
	                output[i].remove(output[i][meta.index(x)])
	        # print(type(output))
        except:
        	pass
        if distinct == True:
            output_set = set() 
            for i in range(len(output[0])):
                l=[]
                for x in range(len(output)):    
                    l.append(output[x][i])
                output_set.add(tuple(l))

            output_set = list(output_set)
            # print(output_set)
            lazy = 0
            for i in column_names:
                if lazy==0:
                    print(i,sep='', end='')
                    lazy = 1
                else :
                    print(','+i,sep='', end='')
            print(' ')
            # print(range(len(output_set)))
            try :

                for i in range(len(output_set)):
                    lazy = 0
                    for x in range(len(output_set[0])):
                        if lazy!=0:
                            print(','+str(output_set[i][x]),sep='', end='')
                        else:
                            lazy = 1
                            print(output_set[i][x],sep='', end=' ')
                    print(' ')
            except:
                print(" ")
        
        else : 
            lazy = 0
            for i in column_names:
                if lazy==0:
                    print(i,sep='', end='')
                    lazy = 1
                else :
                    print(','+i,sep='', end='')
            print(' ')
            
            for i in range(len(output[0])):
                lazy = 0
                for x in range(len(output)):
                    if lazy!=0:
                        print(','+str(output[x][i]),sep='', end='')
                    else:
                        lazy = 1
                        print(output[x][i],sep='', end=' ')
                print(' ')



    else :
        lazy  =0  
        for i in column_names:
            if lazy==0:
                print(i,sep='', end='')
                lazy = 1
            else :
                print(','+i,sep='', end='')
        print(' ')
        lazy = 0
        for i in range(len(output)):
            if lazy!=0:
                    print(','+str(output[i]),sep='', end='')
            else:
                lazy = 1
                print(output[i],sep='', end='')
        print("")
def Execute():
    global identifiers
    global tables
    global meta
    global column_names
    global where
    global output
    global agg
    global distinct
    # print(sql)
    s_query = sql.split(';')
    identifiers[:] = []
    # ; Error handling
    if len(s_query)==1:
        Error_Parsing('No ; present ')
    elif len(s_query) == 2 and len(s_query[1]) !=0 :
        Error_Parsing('Misplaced ; ')
    elif len(s_query)>2:
        Error_Parsing('Too many ; ')
    else:    
        sql_query = ' '.join(s_query)
    
    query = parser(sql_query)

    MetaData()
    ReadTable()
    

    if(identifiers[0].upper() == 'DISTINCT'):
        distinct = True
        identifiers = identifiers[1:]
    
    # print(identifiers)
    if(len(identifiers)<3):
        Error_Parsing('Query invalid !')
        sys.exit()
    
    if identifiers[0].upper() ==  'FROM':
        Error_Parsing('Expect a Attribute list in Select Statement before From... ')
    if identifiers[1].upper() !=  'FROM':
        Error_Parsing('Expect a Correct in Statement ... ')
    
    # print(identifiers)
    
    t = identifiers[2].split(',')
    table_names = [i.strip() for i in t] 
    c = identifiers[0].split(',')
    column_names = [i.strip() for i in c] 
#     print(table_names)
#     print(column_names)
    
    for key in list(tables):
        if key not in table_names:
            tables.pop(key,None)
            meta.pop(key,None)
    
    table_name_check(table_names)
            
#     column_name_check(table_names,column_names)
    if(len(table_names) > 1):
        tables = Join_Table(table_names)
    else:
        tables = tables[table_names[0]]
    
    for k,v in tables.items():
        if type(v) is  list:
            continue
            
        else :
            tables[k]=list(v)
    # print(tables)
    condition = 0
#     x=identifiers[3].split(' ')
    
#     y=[i.strip() for i in x]
    
#     identifiers[3]=""
#     identifiers[3]=''.join(y)
#     print(identifiers[3])
    # if len(identifiers) > 

    if(len(identifiers)>3):
        if (re.search('where .*$',identifiers[3],re.IGNORECASE)):
    #         Where
            identifiers[3] = identifiers[3][len('where') + 1 : -1]
            if(re.search(' AND ',identifiers[3],re.IGNORECASE)):
                where_cond = re.split('AND',identifiers[3],re.IGNORECASE)
                where = [i.strip() for i in where_cond]
                if len(where) !=2:
                    where_cond = re.split('and',identifiers[3],re.IGNORECASE)
                    where = [i.strip() for i in where_cond]
                    if len(where) !=2:
                    # print(where)
                        Error_Parsing('Hmm, Seems your where clause has errors!')
                where[0]=''.join(where[0])
                where[1]=''.join(where[1])
                condition = 1
            elif re.search(' OR ',identifiers[3],re.IGNORECASE):
                where_cond = re.split('OR',identifiers[3],re.IGNORECASE)
                where = [i.strip() for i in where_cond]
                
                if len(where) !=2:
                    where_cond = re.split('or',identifiers[3],re.IGNORECASE)
                    where = [i.strip() for i in where_cond]
                    if len(where) !=2:
                    # print(where)
                        Error_Parsing('Hmm, Seems your where clause has errors!')
                where[0]=''.join(where[0])
                where[1]=''.join(where[1])
                condition = 2
            else:
                where_cond = re.split(' ',identifiers[3])
                where.append(''.join(where_cond))
                if len(where) !=1:
                    # print(where)
                    Error_Parsing('Hmm, Seems your where clause has errors!')
                where[0]=where[0].replace(" ","")
                    
                    
            tables = Process_Where(condition)

    # except:
    #     pass
        
    if(check_star() == False):
        f = column_name_process(table_names)
        column_name_check(table_names)
        # print(tables)
        if f == False:
#             no aggregate functions
#             print(':::',column_names)
            for i in column_names:
                output.append(tables[i])
        else:
            #aggregate functions
            agg = True
            if len(tables[column_names[0]]) !=0:

	            for i in range(len(aggregate)):
	                if aggregate[i] == 'sum':
	                    x = sum(tables[column_names[i]])
	                    output.append(x)
	                if aggregate[i] == 'max':
	                    x = max(tables[column_names[i]])
	                    output.append(x)
	                if aggregate[i] == 'min':
	                    x = min(tables[column_names[i]])
	                    output.append(x)
	                if aggregate[i] == 'avg':
	                    x = sum(tables[column_names[i]])
	                    x = x/len(tables[column_names[i]])
	                    output.append(x)
	       
	        # else:
	        # 	pass
		
    else:
        for i in tables:
            output.append(tables[i])
    
    Project_Table()

#     print(output)
#     print(where)
    

def main():
    Execute()

if __name__ == "__main__":
    main()

