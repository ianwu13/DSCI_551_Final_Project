from sre_parse import State
import sqlalchemy as db
import pymysql
from sqlalchemy.sql import text
import pandas as pd

sql_server = 'mysql://root:weiye0726@localhost/project'
pymysql.install_as_MySQLdb()
engine = db.create_engine(sql_server)
connection = engine.connect()
metadata = db.MetaData()
schema_name = sql_server.split("/")[-1]

def create_dir_structure_table():
    ds = db.Table(
        "directory_structure",
        metadata,
        db.Column("parent", db.String(50)),
        db.Column("children", db.String(50))
    )
    metadata.create_all(engine)
    print ("Directory Structure created")


def create_directory(dir_name:str):
    full_path = filter(None, dir_name.split('/'))
    paths = list(full_path)
    if len(paths) == 0:
        print("please specify path")
        return
    elif len(paths) == 1:
        print("Case for 1")
        print ("check if exists")
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[0])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[0] in output:
            print("already exists")
            return
        root = paths[0]
        stmt = "insert into directory_structure (parent, children) values (NULL,"+"'{}'".format(root)+")"
        engine.execute(stmt)
    else:
        print("case for multiple")
        print("check for valid path")
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[-2])+";"

        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[-2] not in output:
            print("invalid path")
            return
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[-1])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[-1] in output:
            print("already exists")
            return
        stmt = "insert into directory_structure (parent, children) values ("+"'{}'".format(paths[-2])+", "+"'{}'".format(paths[-1])+")"
        output = engine.execute(stmt)

def mkdir(dir_path:str):
    insp = db.inspect(engine)
    if insp.has_table("directory_structure", schema=schema_name) == True:
        create_directory(dir_path)
    else:
        create_dir_structure_table()
        create_directory(dir_path)

# mkdir("/path")

def check_valid_path(dir_name:str):
    full_path = filter(None, dir_name.split('/'))
    paths = list(full_path)
    stmt = "select parent from directory_structure where children ="+"'{}'".format(paths[-1])+";"
    output = engine.execute(stmt)
    output = [i[0] for i in output][0]
    stmt = "select parent from directory_structure where parent ="+"'{}'".format(output)+";"
    output = engine.execute(stmt)
    output = [i[0] for i in output]
    if len(output) >0 and output[0] not in paths:
        # print("invalid path")
        return False
    else:
        # print("valid path")
        return True

# check_valid_path("/path/test/john/")

def ls(dir_path:str):
    full_path = filter(None, dir_path.split('/'))
    paths = list(full_path)
    if check_valid_path(dir_path) == False:
        return
    else:
        stmt = "select children from directory_structure where parent ="+"'{}'".format(paths[-1])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        for i in output:
            print (i)

def load(table_name:str, file_name:str):
    df = pd.read_csv(file_name)
    df.to_sql(con=engine, index_label='id', name=table_name, if_exists='replace')

def put(dir_path:str):
    full_path = filter(None, dir_path.split("/"))
    paths = list(full_path)
    file_name = paths[-1]
    paths = '/'+'/'.join(paths[:-1])
    if check_valid_path(paths) == False:
        return
    else:
        insp = db.inspect(engine)
        table_name = file_name.split(".")[0]
        if insp.has_table(table_name, schema=schema_name) == True:
            print("file exists")
        else:
            print("load file")
            load(table_name,file_name)
            parent = paths.split("/")[-1]
            stmt = "insert into directory_structure (parent, children) values ("+"'{}'".format(parent)+","+"'{}'".format(file_name)+")"
            engine.execute(stmt)
    return

def cat(dir_path:str):
    full_path = filter(None, dir_path.split("/"))
    paths = list(full_path)
    file_name = paths[-1]
    paths = '/'+'/'.join(paths[:-1])
    if check_valid_path(paths) == False:
        print("invalid path")
        return
    else:
        insp = db.inspect(engine)
        table_name = file_name.split(".")[0]

        if insp.has_table(table_name, schema=schema_name) == True:
            print("file exists")
            df = pd.read_sql_query("select * from co2;",con=connection)
            print(df)
        else:
            print("file not exists")

def rm(dir_path:str):
    full_path = filter(None, dir_path.split("/"))
    paths = list(full_path)
    file_name = paths[-1]
    table_name = paths[-1].split(".")[0]
    paths = '/'+'/'.join(paths[:-1])
    if check_valid_path(paths) == False:
        print("invalid path")
        return
    else:
        insp = db.inspect(engine)
        table_name = file_name.split(".")[0]

        if insp.has_table(table_name, schema=schema_name) == True:
            print("file exists")
            stmt = "drop table "+table_name+";"
            engine.execute(stmt)
            print("file removed")
        else:
            print("file not exists")

test_csv = "co2.csv"
# rm("/path/test/"+test_csv)
# put("/path/test/"+test_csv)
# rm("/path/test/"+test_csv)

# ls("path")

# create_dir_structure_table()

# create_directory("/path/final")

# def mkdir(dir_name:str):
#     full_path = filter(None, dir_name.split('/'))
#     paths = list(full_path)
#     if len(paths) == 1:
#         table_name = paths[0]
#         insp = db.inspect(engine)
#         if insp.has_table(table_name, schema=schema_name) == True:
#             print (table_name,"exists")

#         else:
#             dir_name = db.Table(
#                 table_name,
#                 metadata,
#                 db.Column("parent", db.String(100)),
#                 db.Column("children", db.String(100))
#             )
#             metadata.create_all(engine)
#             print (table_name,"created")
#             engine.execute("insert into "+paths[0]+" (parent,children) values (NULL,NULL);")
#     else:
#         for i in range(len(paths)):
#             # print(paths[i])
#             table_name = paths[i]

#             insp = db.inspect(engine)
#             if insp.has_table(paths[i], schema=schema_name) == True:
#                 print (paths[i],"exists")
                # print("insert into "+paths[i]+" (parent,children) values (NULL,"+paths[i+1]+");")

                # print("insert into "+paths[i-1]+" (parent,children) values (NULL,"+paths[i]+");")
                # engine.execute("insert into "+paths[i]+" (parent,children) values (NULL,"+paths[i+1]+");")

            # else:
            #     print (paths[i],"not exists")

            #     dir_name = db.Table(
            #         table_name,
            #         metadata,
            #         db.Column("parent", db.String(100)),
            #         db.Column("children", db.String(100))
            #     )
            #     metadata.create_all(engine)
            #     print("insert into "+paths[i-1]+" (parent,children) values (NULL,"+paths[i]+");")

            #     engine.execute("insert into "+paths[i-1]+" (parent,children) values (NULL,"+paths[i]+");")

        # print (dir_name)

    # for i in paths:
    #     insp = db.inspect(engine)
    #     if insp.has_table(i, schema=schema_name) == True:
    #         print (i,"exists")
    #     else:
    #         print (i,"not exists")


            # return
    # print (paths)
    # table_name = dir_name.split("/")[1]
    # insp = db.inspect(engine)
    # if insp.has_table(table_name, schema=schema_name) == True:
    #     print (table_name,"exists")
    #     return
    # else:
    #     dir_name = db.Table(
    #         table_name,
    #         metadata,
    #         db.Column("parent", db.String(100)),
    #         db.Column("children", db.String(100))
    #     )
    #     metadata.create_all(engine)
    #     print (table_name,"created")
    # return 
    # print (dir_name)
    
# mkdir("/user/john")

# Can execute any MySQL queries
def query(statement:str):
    with engine.connect() as connection:
        output = connection.execute(statement)
        for i in output:
            print (i)

# query("select * from directory_structure")

# # Create and load table
# def load(table_name:str, file_name:str):
#     df = pd.read_csv(file_name)
#     df.to_sql(con=engine, index_label='id', name='ETS', if_exists='replace')


# load('ETS','https://pkgstore.datahub.io/core/eu-emissions-trading-system/eu-ets_csv/data/c23b15dd88b91e57d0e30ec82869c9f0/eu-ets_csv.csv')
# def create_table():
#     print ("create table")
    # user = db.Table(
    #     "ETS",
    #     metadata,
    #     db.Column("country_code", db.Integer),
    #     db.Column("country", db.String(20)),
    #     db.Column("main activity sector name", db.String(200)),
    #     db.Column("ETS information", db.String(200)),
    #     db.Column("year", db.Integer),
    #     db.Column("value", db.Integer),
    #     db.Column("unit", db.String(100))
    # )
    # metadata.create_all(engine)
# query("drop table /user")