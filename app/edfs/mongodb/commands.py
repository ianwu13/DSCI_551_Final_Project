import pymongo
from pymongo import MongoClient

from edfs.creds import MG_CLIENT_ARGS, MG_DB_NAME


NUM_DATANODES = 2

client = MongoClient(MG_CLIENT_ARGS[0], MG_CLIENT_ARGS[1])
db = client[MG_DB_NAME]

namenode_collection = db['namenode']
datanode_collections = [db[f'datanode_{i+1}'] for i in range(NUM_DATANODES)]


def preprocess_path(path: str):
    if path == '.':
        return 'namenode/root/'
    elif path[-1] == '/':
        path = path[:-1]
    if path[:6] == '/root/':
        path = path[6:]
    elif path[0] == '/':
        path = path[1:]
    return ''.join(['namenode/root/', path])

def mkdir(path:str):
    
    if '.' in path:
        return ('INVALID PATH')
    else:
        path = preprocess_path(path)

    path_split = [s for s in path.split('/') if s != '']

    # check if path exists:
    new_path = []
    exists = False
    for i in range(2, len(path_split)+1):
        check_path = '.'.join(path_split[1:i])
        if list(db.namenode.find({check_path: {'$exists': True}})) == []:
            exists = False
            new_path.append(check_path)
        else: 
            exists = True
    if exists:
        return ('Path already exist')
    
    # create record for new path
    else:
        for newpath in new_path:
            db.namenode.update_one({newpath.rsplit('.', 1)[0]: {'$exists': True}}, 
                                   {'$set': {newpath: {}}})

    return ('Path Created')

def ls(path: str): 
    
    # listing content of a given directory, e.g., ls /user

    if '.' in path:
        return 'NOT A DIRECTORY'
    
    path = preprocess_path(path)
    path_split = [s for s in path.split('/') if s != '']
    namenode_collection = db['namenode']
    cursor = namenode_collection.find({})
    
    for document in cursor:
        exist = True
        cur_dict = document
        for s in path_split[1:]:
            if s in cur_dict:
                cur_dict = cur_dict[s]
                keys = list(cur_dict.keys())
            else:
                exist = False
                break
        if exist:
            if keys == [ ]:
                return
            else:
                return ', '.join(keys)
            break

    if not exist:
        return ('NOT A DIRECTORY')


def cat(path: str): 
    # display content of a file, e.g., cat /user/john/hello.txt
    pass


def rm(path: str): 
    # remove a file from the file system, e.g., rm /user/john/hello.txt
    pass

def check_exists(dest_path: str):
    path = preprocess_path(dest_path)
    path_split = [s for s in path.split('/') if s != '']
    check_path = '.'.join(path_split[1:len(path_split)])
    if list(db.namenode.find({check_path: {'$exists': True}})) != []:
        return True
    else:
        return False

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

    # Check if file already exists
    if check_exists(dest_path):
        return 'FILE ALREADY EXISTS IN EDFS'
    else:
        dest_split = [s.replace('.', '-') for s in dest_path.split('/') if s != '']
        check_path = '.'.join(dest_split[1:-1])
        new_path = '.'.join(dest_split[1:])
        
        try:
            df = pd.read_csv(file_path)
#             df = df.head(15)
            df.index = pd.RangeIndex(start=1, stop=len(df)+1)
        except:
            return 'INVALID FILE PATH'

        if len(df) < k:
            k = len(df)
        
        dn = 1
        for p in range(k):
            partition_num = 1
            records = df.iloc[p::k, :].to_dict(orient='records') # split of data seems not accurate, double check
            if p%NUM_DATANODES == 0:
                dn = 1
                if p != 0:
                    partition_num += 1
            else:
                dn += 1
            datanode = f'datanode{dn}'
            db[datanode].insert_many([{f'p{partition_num}': records}])
            
            # create records in namenode
            db.namenode.update_many({check_path: {'$exists': True}}, {'$set': {f'{new_path}.p{p+1}': datanode}})

        return 'FILE UPLOADED SUCCESFULLY'


def getPartitionLocations(path: str): 
    # this method will return the locations of partitions of the file.
    pass


def readPartition(path: str, partition: int):
    '''
    this method will return the content of partition # of
    the specified file. The portioned data will be needed in the second task for parallel
    processing.
    '''
    pass
