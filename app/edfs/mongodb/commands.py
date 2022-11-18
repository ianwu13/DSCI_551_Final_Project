import pymongo
import pandas as pd
from pymongo import MongoClient

from edfs.creds import MG_CLIENT_ARGS, MG_DB_NAME


NUM_DATANODES = 2

client = MongoClient(MG_CLIENT_ARGS[0], MG_CLIENT_ARGS[1])
db = client[MG_DB_NAME]

namenode_collection = db['namenode']
datanode_collections = [db[f'datanode_{i+1}'] for i in range(NUM_DATANODES)]

# CHECK IF ROOT EXISTS IN NAMENODE AND CREATE IF NOT
if list(namenode_collection.find({'parent': None, 'child': 'root'})) == []:
    namenode_collection.insert_one({'parent': None, 'child': 'root'})


def get_next_datanode(cur_datanode: int):
    return (cur_datanode) % NUM_DATANODES + 1


def preprocess_path(path: str):
    if path == '.':
        return 'root/'
    elif path[-1] == '/':
        path = path[:-1]
    if path and path[0] == '/':
        path = path[1:]
    if path[:4] == 'root':
        path = path[4:]
        if path and path[0] == '/':
            path = path[1:]
    if not path:
        return 'root'

    return ''.join(['root/', path])


def mkdir(path:str):
    if '.' in path:
        return ('INVALID PATH')
    else:
        path = preprocess_path(path)
        if path == 'root/':
            return ('INVALID PATH')

    splt = path.split('/')

    if list(namenode_collection.find({'parent': splt[-2], 'child': splt[-1]})) != []:
        return 'PATH ALREADY EXISTS'
    elif list(namenode_collection.find({'child': splt[-1]})) != []:
        return 'CANNOT CREATE DIRECTORIES/FILES WITH DUPLICATE NAMES'

    parent_dir = 'root'
    for child_dir in splt[1:]:
        if list(namenode_collection.find({'parent': parent_dir, 'child': child_dir})) == []:
            namenode_collection.insert_one({'parent': parent_dir, 'child': child_dir})
        parent_dir = child_dir

    return 'DIRECTORY CREATED'


def ls(path: str): 
    # listing content of a given directory, e.g., ls /user
    if '.' in path:
        return ('INVALID PATH')
    else:
        path = preprocess_path(path)

    splt = path.split('/')

    parent_dir = None
    for child_dir in splt:
        if list(namenode_collection.find({'parent': parent_dir, 'child': child_dir})) == []:
            return 'PATH DOES NOT EXIST'
        parent_dir = child_dir

    contents = list(namenode_collection.find({'parent': parent_dir}))
    return '\n'.join([i['child'] for i in contents])


def put(file_path: str, dest_path: str, k: str): 
    try:
        k = int(k)
    except:
        return 'ARGUMENT FOR K MUST BE INTEGER'

    f_name = file_path.split('/')[-1]
    if f_name.split('.')[-1] != 'csv':
        return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'

    if '.' not in dest_path.split('/')[-1]:
        dest_path = f'{preprocess_path(dest_path)}/{f_name}'
    else:
        if dest_path.split('.')[-1] != 'csv':
            return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'
        dest_path = preprocess_path(dest_path)

    # Check if file already exists
    splt = dest_path.split('/')
    if list(namenode_collection.find({'parent': splt[-2], 'child': splt[-1]})) != []:
        return 'FILE ALREADY EXISTS IN EDFS'
    elif list(namenode_collection.find({'child': splt[-1]})) != []:
        return 'CANNOT CREATE FILES/DIRECTORIES WITH DUPLICATE NAMES'
    else:
        mkdir('/'.join(splt[:-1]))
        try:
            df = pd.read_csv(file_path)
            df.index = pd.RangeIndex(start=1, stop=len(df)+1)
        except:
            return 'INVALID FILE PATH'

        namenode_entry = {'parent':splt[-2], 'child': splt[-1]}

        if len(df) < k:
            k = len(df)
        cur_datanode = 1
        for p in range(k):
            datanode = f'datanode_{cur_datanode}'
            dn_coll = datanode_collections[cur_datanode-1] # -1 because 0 index
            dn_b_num = dn_coll.count_documents({})

            namenode_entry[f'p{p+1}'] = f'{datanode}-{dn_b_num}'

            data = df.iloc[p::k, :].to_dict(orient="index")
            data = {str(k):v for k,v in data.items()}
            dn_coll.insert_one({'b_num': dn_b_num, 'data': data})

            cur_datanode = get_next_datanode(cur_datanode)

        namenode_collection.insert_one(namenode_entry)
        return 'FILE UPLOADED SUCCESFULLY'


def getPartitionLocations(path: str): 
    # this method will return the locations of partitions of the file.
    if '.' not in path:
        return 'PATH MUST BE A FILE'

    splt = preprocess_path(path).split('/')

    try:
        partitions = list(namenode_collection.find({'parent': splt[-2], 'child': splt[-1]}))[0]
        partitions = [partitions[f'p{i+1}'] for i in range(len(partitions)-3)]
    except:
        return 'NO DATA FOUND FOR FILE'

    return '\n'.join(partitions)


def cat(path: str): 
    # display content of a file, e.g., cat /user/john/hello.txt
    if '.' not in path.split('/')[-1]:
        return 'PATH MUST BE A FILE'
    path = preprocess_path(path)

    partitions = getPartitionLocations(path).split('\n')
    data = []
    for p in partitions:
        p_info = p.split('_')[1].split('-')
        data.append(list(list(datanode_collections[int(p_info[0])-1].find({'b_num': int(p_info[1])}))[0]['data'].values()))

    output = ', '.join(data[0][0].keys())
    for i in range(len(data[0])):
        for j in range(len(data)):
            try:
                line = data[j][i]
            except:
                return output
            output = '\n'.join([output, ', '.join([str(v) for v in line.values()])])
    return output


def recursive_remover(f_name: str):
    if '.' in f_name:
        # Handle File
        partitions = list(namenode_collection.find({'child': f_name}))[0]
        partitions = [partitions[f'p{i+1}'] for i in range(len(partitions)-3)]
        for p in partitions:
            # Remove Partition Data
            p_info = p.split('_')[1].split('-')
            datanode_collections[int(p_info[0])-1].delete_one({'b_num': int(p_info[1])})

    else:
        # Handle Dir
        output = list(namenode_collection.find({'parent': f_name}))
        children = [i['child'] for i in output]
        for c in children:
            recursive_remover(c)
    
    namenode_collection.delete_one({'child': f_name})


def rm(dir_path:str):
    path = preprocess_path(dir_path)

    splt = path.split('/')

    parent_dir = None
    for child_dir in splt:
        if list(namenode_collection.find({'parent': parent_dir, 'child': child_dir})) == []:
            return 'PATH DOES NOT EXIST'
        parent_dir = child_dir

    recursive_remover(parent_dir)

    return 'FILE REMOVED'


def readPartition(path: str, partition: int):
    '''
    this method will return the content of partition # of
    the specified file. The portioned data will be needed in the second task for parallel
    processing.
    '''
    if '.' not in path.split('/')[-1]:
        return 'PATH MUST BE A FILE'
    path = preprocess_path(path)

    partitions = getPartitionLocations(path).split('\n')
    if partition not in partitions:
        return 'INVALID PARTITION'

    p_info = partition.split('_')[1].split('-')
    data = list(datanode_collections[int(p_info[0])-1].find({'b_num': int(p_info[1])}))[0]['data']

    if not data:
        return 'INVALID PARTITION'
    else:
        data = list(data.values())
    output = ', '.join(data[0].keys())
    for d in data:
        output = '\n'.join([output, ', '.join([str(v) for v in d.values()])])
    return output
