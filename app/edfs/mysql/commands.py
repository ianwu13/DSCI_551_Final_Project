import sqlalchemy as db
from sqlalchemy_utils import database_exists, create_database
import pymysql
import pandas as pd

from edfs.creds import SQL_SERVER_STRING


NUM_DATANODES = 2


def get_next_datanode(cur_datanode: int):
    return (cur_datanode) % NUM_DATANODES + 1


def create_namenode_structure():
    # Create directory structure if not exists
    insp = db.inspect(engine)
    if (insp.has_table("directory_structure", schema=schema_name)) and (insp.has_table("partition_list", schema=schema_name)):
        return 'namenode structure already exists'
    elif insp.has_table("directory_structure", schema=schema_name):
        stmt = "drop table directory_structure;"
        engine.execute(stmt)
    elif insp.has_table("partition_list", schema=schema_name):
        stmt = "drop table partition_list;"
        engine.execute(stmt)

    metadata = db.MetaData()
    ds = db.Table(
        "directory_structure",
        metadata,
        db.Column("parent", db.String(50)),
        db.Column("children", db.String(50)),
        db.Column("id", db.Integer, db.Identity(start=0, cycle=True), primary_key=True)
    )
    pl = db.Table(
        "partition_list",
        metadata,
        db.Column("file_id", db.Integer),
        db.Column("partition_ptr", db.String(50)),
        db.ForeignKeyConstraint(
            ["file_id"], ["directory_structure.id"]
        )
    )
    metadata.create_all(engine)
    stmt = "insert into directory_structure (parent, children) values (NULL,'root');"
    engine.execute(stmt)

    return "NAMENODE STRUCTURE CREATED"


def create_datanode_table(dn_num: int):
    datanode_name = f'datanode_{dn_num}'

    # Check if datanode exists
    if db.inspect(engine).has_table(datanode_name, schema=schema_name):
        return f"Datanode {dn_num} Already exists"

    metadata = db.MetaData()
    dn = db.Table(
        datanode_name,
        metadata,
        db.Column("p_id", db.Integer, db.Identity(start=0, cycle=True), primary_key=True),
        db.Column("data", db.Text())
    )
    metadata.create_all(engine)

    return "DATANODE CREATED"


# Initialize database
pymysql.install_as_MySQLdb()

sql_server = SQL_SERVER_STRING
schema_name = sql_server.split("/")[-1]

engine = db.create_engine(SQL_SERVER_STRING)
# Create database if it does not exist.
if not database_exists(engine.url):
    create_database(engine.url)
connection = engine.connect()

create_namenode_structure()
for i in range(1, NUM_DATANODES+1):
    create_datanode_table(i)


def preprocess_path(path: str):
    if path == '.':
        return 'ROOT/'
    elif path[-1] == '/':
        path = path[:-1]
    if path and path[0] == '/':
        path = path[1:]
    if path[:4].upper() == 'ROOT':
        path = path[4:]
        if path and path[0] == '/':
            path = path[1:]
    
    return ''.join(['ROOT/', path])


def create_directory(dir_name:str):
    full_path = filter(None, dir_name.split('/'))
    paths = list(full_path)
    if len(paths) == 0:
        # print("please specify path")
        return "NO SPECIFIED PATH"
    elif len(paths) == 1:
        # print("Case for 1")
        # print ("check if exists")
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[0])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[0] in output:
            return "ALREADY EXISTS"
        root = paths[0]
        stmt = "insert into directory_structure (parent, children) values (NULL,"+"'{}'".format(root)+")"
        engine.execute(stmt)
    else:
        # print("case for multiple")
        # print("check for valid path")
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[-2])+";"

        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[-2] not in output:
            return "PARENT DIRECTORY DOES NOT EXIST YET"
        stmt = "select children from directory_structure where children ="+"'{}'".format(paths[-1])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        if paths[-1] in output:
            return "DIRECTORY ALREADY EXISTS"
        stmt = "insert into directory_structure (parent, children) values ("+"'{}'".format(paths[-2])+", "+"'{}'".format(paths[-1])+")"
        output = engine.execute(stmt)
    return "DIRECTORY CREATED"


def mkdir(dir_path:str):
    if '.' in dir_path:
        return 'INVALID PATH'
    else:
        dir_path = preprocess_path(dir_path)

    res_message = create_directory(dir_path)
    return res_message


def path_exists(path:str):
    paths = list(filter(None, path.split('/')))
    cur_parent = paths[0]
    x = 1
    if cur_parent != 'ROOT':
        cur_parent = 'ROOT'
        x = 0
    # If path is just ROOT
    elif len(paths) == 1:
        return True
    for cur_child in paths[x:]:
        stmt = f"select children from directory_structure where parent = '{cur_parent}' and children = '{cur_child}';"
        if len(engine.execute(stmt).all()) == 0:
            return False # directory path does not exist
        cur_parent = cur_child
    return True


def ls(dir_path:str):
    if '.' in dir_path:
        return 'NOT A DIRECTORY'

    dir_path = preprocess_path(dir_path)

    full_path = filter(None, dir_path.split('/'))
    paths = list(full_path)
    if path_exists(dir_path) == False:
        return ""
    else:
        stmt = "select children from directory_structure where parent ="+"'{}'".format(paths[-1])+";"
        output = engine.execute(stmt)
        output = [i[0] for i in output]
        return '\n'.join(output)


def load(table_name:str, file_name:str):
    df = pd.read_csv(file_name)
    df.to_sql(con=engine, index_label='id', name=table_name, if_exists='replace')


def put(file_path: str, destination_path:str, k: str):
    try:
        k = int(k)
    except:
        return 'ARGUMENT FOR K MUST BE INTEGER'

    f_name = file_path.split('/')[-1]
    if f_name.split('.')[-1] != 'csv':
        return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'

    if '.' not in destination_path.split('/')[-1]:
        destination_path = f'{preprocess_path(destination_path)}/{f_name}'
    else:
        if destination_path.split('.')[-1] != 'csv':
            return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'
        destination_path = preprocess_path(destination_path)
    
    # CHECK EXISTS preprocess_path(destination_path)
    paths = '/'.join(['', *destination_path.split('/')[:-1]])
    if not path_exists(paths):
        return "DESTINATION DIRECTORY MUST ALREADY EXIST"
    # Check if file already exists
    elif path_exists(destination_path):
        return 'FILE ALREADY EXISTS IN EDFS'
    else:
        # Create file in directory structure
        paths = destination_path.split('/')
        dest_dir = paths[-2]
        dest_file = paths[-1]
        engine.execute(f"insert into directory_structure (parent, children) values ('{dest_dir}', '{dest_file}');")
        dir_struct_id = engine.execute('SELECT LAST_INSERT_ID() AS id').fetchone()['id']

        try:
            df = pd.read_csv(file_path)
            df.index = pd.RangeIndex(start=1, stop=len(df)+1)
        except:
            return 'INVALID FILE PATH'

        if len(df) < k:
            k = len(df)
        cur_datanode = 1
        for p in range(k):
            datanode = f'datanode_{cur_datanode}'

            # Convert data into a string
            data = df.iloc[p::k, :]
            data_string = '\n'.join([','.join(data.columns), *[','.join(row) for row in data.to_numpy(dtype=str)]])

            # INSERT "data_string" into  "data" column of proper datanode table 
            engine.execute(f"insert into {datanode} (data) values ('{data_string}');")
            p_num = engine.execute('SELECT LAST_INSERT_ID() AS id').fetchone()['id']

            # "dir_struct_id" = file_id for newly created file
            cur_partition_ptr = f'{datanode}-{p_num}'
            engine.execute(f"insert into partition_list values ({dir_struct_id}, '{cur_partition_ptr}');")

            cur_datanode = get_next_datanode(cur_datanode)

        return 'FILE UPLOADED SUCCESFULLY'


def cat(path:str):
    path = preprocess_path(path)
    if not path_exists(path):
        return 'INVALID PATH'
    else:
        # Get pointers for file with getPartitionLocations
        pointers = getPartitionLocations(path).split('\n')

        data = []
        for p in pointers:
            splt = p.split('-')
            node = splt[0]
            p_num = int(splt[1])

            d = engine.execute(f"select data from {node} where p_id ={p_num};").fetchone()[0]
            data.append(d)
        
        data = [d.split('\n') for d in data]
        output = data[0][0]
        for i in range(1, len(data[0])):
            for j in range(len(data)):
                try:
                    line = data[j][i]
                except:
                    return output
                output = '\n'.join([output, line])
        return output


def recursive_remover(f_name: str):
    if '.' in f_name:
        # Handle File
        stmt = f"select id from directory_structure where children = '{f_name}';"
        file_id = engine.execute(stmt).fetchone()[0]

        stmt = f"select partition_ptr from partition_list where file_id = {file_id};"
        output = engine.execute(stmt)
        ptrs = [i[0] for i in output]

        for pt in ptrs:
            splt = pt.split('-')
            node = splt[0]
            pid = int(splt[1])
            stmt = f"delete from {node} where p_id = {pid};"
            engine.execute(stmt)

        stmt = f"delete from partition_list where file_id = {file_id};"
        engine.execute(stmt)
    else:
        stmt = f"select children from directory_structure where parent = '{f_name}';"
        output = engine.execute(stmt)
        children = [i[0] for i in output]
        for c in children:
            recursive_remover(c)
    
    stmt = f"delete from directory_structure where children = '{f_name}';"
    engine.execute(stmt)


def rm(dir_path:str):
    path = preprocess_path(dir_path)
    if not path_exists(path):
        return 'PATH DOES NOT EXIST'
    
    recursive_remover(path.split('/')[-1])

    return 'FILE REMOVED'


def getPartitionLocations(path: str): 
    # this method will return the locations of partitions of the file.
    path = preprocess_path(path)
    if not path_exists(path):
        return 'INVALID PATH'

    f_name = path.split('/')[-1]
    if '.' not in f_name:
        return 'PATH MUST BE A FILE' 
    stmt = f"select id from directory_structure where children = '{f_name}';"
    file_id = engine.execute(stmt).fetchone()[0]
    stmt = f"select partition_ptr from partition_list where file_id = {file_id};"
    output = engine.execute(stmt)
    output = [i[0] for i in output]
    return '\n'.join(output)


def readPartition(path: str, partition: str):
    '''
    this method will return the content of partition # of
    the specified file. The portioned data will be needed in the second task for parallel
    processing.
    '''
    path = preprocess_path(path)
    if not path_exists(path):
        return 'PATH DOES NOT EXIST'
    elif '.' not in path.split('/')[-1]:
        return 'PATH MUST BE A FILE, NOT DIRECTORY'

    splt = partition.split('-')
    node = splt[0]
    p_num = int(splt[1])

    d = engine.execute(f"select data from {node} where p_id ={p_num};").fetchone()[0]
    return d
