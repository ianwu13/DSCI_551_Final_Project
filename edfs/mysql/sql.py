from sre_parse import State
import sqlalchemy as db
import pymysql
from sqlalchemy.sql import text
import pandas as pd

sql_server = "your_sql_server"
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

# Can execute any MySQL queries
def query(statement:str):
    with engine.connect() as connection:
        output = connection.execute(statement)
        for i in output:
            print (i)
