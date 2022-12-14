import requests as r
import json
import pandas as pd

from edfs.creds import FB_BASE_URL


NUM_DATANODES = 2


def get_next_datanode(cur_datanode: int):
    return (cur_datanode) % NUM_DATANODES + 1
    

def get_next_b_num(dn: str):
    b = r.get(f'{FB_BASE_URL}{dn}/next_block.json').text
    if b == 'null':
        r.put(f'{FB_BASE_URL}{dn}/next_block.json', '2')
        return 1
    else:
        r.put(f'{FB_BASE_URL}{dn}/next_block.json', f'{int(b)+1}')
        return int(b)


def preprocess_path(path: str):
    if path == '.':
        return 'namenode/root/'
    elif path[-1] == '/':
        path = path[:-1]
    if path and path[0] == '/':
        path = path[1:]
    if path[:4] == 'root':
        path = path[4:]
        if path and path[0] == '/':
            path = path[1:]
    
    return ''.join(['namenode/root/', path])


def check_exists(dest_path: str):
    if r.get(dest_path).text != 'null':
        return True
    else:
        return False


def handle_empty_dir(dir_path: str):
    if r.get(dir_path).text == '0':
        r.delete(dir_path)
        return True
    else:
        return False


def mkdir(path: str): 
    # create a directory in file system, e.g., mkdir /user/john

    if '.' in path:
        return 'INVALID PATH'
    else:
        path = preprocess_path(path)

    # Check if directory already exists
    if check_exists(f'{FB_BASE_URL}{path}.json'):
        return 'DIRECTORY ALREADY EXISTS'
    else:
        r.put(f'{FB_BASE_URL}{path}.json', '0')
        return 'DIRECTORY CREATED SUCCESFULLY'


'''
Example:

ls('user/home') --> 
    tmp = data_from_home_in_fb
    for f in tmp.keys():
        print(f)


currently in firebase:
    root: {
        user: {
            home: {
                file1: {}
                file2: {}
            }
        }
}
'''
def ls(path: str): 
    # listing content of a given directory, e.g., ls /user

    if '.' in path:
        return 'NOT A DIRECTORY'

    path = preprocess_path(path)
    keys = json.loads(r.get(f'{FB_BASE_URL}{path}.json').text)
    if keys == 0:
        return ''
    elif not keys:
        return 'NO SUCH DIRECTORY'
    return '\n'.join([k.replace('-', '.') for k in keys])


def cat(path: str): 
    # display content of a file, e.g., cat /user/john/hello.txt
    if '.' not in path.split('/')[-1]:
        return 'PATH MUST BE A FILE'
    path = preprocess_path(path.replace('.', '-'))

    partitions = json.loads(r.get(f'{FB_BASE_URL}{path}.json').text)
    if partitions == 'null' or partitions == None:
        return 'FILE NOT FOUND'
    data = []
    for p in partitions:
        tmp = json.loads(r.get(f'{FB_BASE_URL}{partitions[p].replace("-", "/")}.json').text)
        if type(tmp) == dict:
            data.append(list(tmp.values()))
        elif type(tmp) == list:
            data.append(list(filter(lambda x: x is not None, tmp)))

    output = ', '.join(data[0][0].keys())
    for i in range(len(data[0])):
        for j in range(len(data)):
            try:
                line = data[j][i]
            except:
                return output
            output = '\n'.join([output, ', '.join([str(v) for v in line.values()])])
    return output


def rm(path: str): 
    # remove a file from the file system, e.g., rm /user/john/hello.txt

    if '.' not in path.split('/')[-1]:
        # Handle Directory
        path = preprocess_path(path)
        parent_path = '/'.join(path.split('/')[:-1])
        if len(json.loads(r.get(f'{FB_BASE_URL}{parent_path}.json').text)) < 2:
            r.put(f'{FB_BASE_URL}{parent_path}.json', '0')
        else:
            r.delete(f'{FB_BASE_URL}{path}.json')
        
        return 'DONE - NOTE: PARTITIONS FOR SUBFILES NOT DELETED'
    else:
        # Handle File
        path = preprocess_path(path).replace('.', '-')
        partitions = json.loads(r.get(f'{FB_BASE_URL}{path}.json').text).values()
        for p in partitions:
            r.delete(f'{FB_BASE_URL}{p.replace("-", "/")}.json')

        parent_path = '/'.join(path.split('/')[:-1])
        if len(json.loads(r.get(f'{FB_BASE_URL}{parent_path}.json').text)) < 2:
            r.put(f'{FB_BASE_URL}{parent_path}.json', '0')
        else:
            r.delete(f'{FB_BASE_URL}{path}.json')

        return 'DONE'


'''
Firebase EDFS Architecture
firebase database: 
----------------------
{
"current_partition_pointer": 125,
"root": {
    "user": {
        "file1.txt": {"p1": 123, "p2": 124}
    }
},
123: {SOME DATA},
124: {SOME DATA}
}
----------------------
call put('file2.txt', 'user/', 2)
    now get file2.txt from local or wherever
    go to firebase
    insert *"file2.txt": {"p1": 125, "p2": 126}*
'''
def put(file_path: str, destination_path: str, k: str): 
    '''
    uploading a file to file system, e.g., put(cars.csv, /user/john, k = # partitions) will
    upload a file cars.csv to the directory /user/john in EDFS. But note that the file
    should be stored in k partitions, and the file system should remember where the
    partitions are stored. You should design a method to partition the data. You may
    also have the user indicate the method, e.g., hashing on certain car attribute, in the
    put method. 
    '''
    try:
        k = int(k)
    except:
        return 'ARGUMENT FOR K MUST BE INTEGER'

    f_name = file_path.split('/')[-1].replace('.', '-')
    if f_name.split('-')[-1] != 'csv':
        return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'

    if '.' not in destination_path.split('/')[-1]:
        destination_path = f'{preprocess_path(destination_path)}/{f_name}'
    else:
        if destination_path.split('.')[-1] != 'csv':
            return 'INVALID FILE TYPE, ONLY CSV IS CURRENTLY SUPPORTED'
        destination_path = preprocess_path(destination_path).replace('.', '-')

    # Check if file already exists
    if check_exists(f'{FB_BASE_URL}{destination_path}.json'):
        return 'FILE ALREADY EXISTS IN EDFS'
    else:
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
            dn_b_num = get_next_b_num(datanode)
            r.put(f'{FB_BASE_URL}{destination_path}/p{p+1}.json', f'"{datanode}-{dn_b_num}"')

            data = df.iloc[p::k, :].to_json(orient="index")
            r.put(f'{FB_BASE_URL}{datanode}/{dn_b_num}.json', str(data))

            cur_datanode = get_next_datanode(cur_datanode)

        return 'FILE UPLOADED SUCCESFULLY'


def getPartitionLocations(path: str): 
    # this method will return the locations of partitions of the file.

    if '.' not in path:
        return 'PATH MUST BE A FILE'

    path = preprocess_path(path).replace('.', '-')
    partitions = json.loads(r.get(f'{FB_BASE_URL}{path}.json').text)
    if (not (type(partitions) == dict)) or len(partitions) == 0:
        return 'NO DATA FOUND FOR FILE'

    return '\n'.join(partitions.values())


def readPartition(path: str, partition: str):
    '''
    this method will return the content of partition # of
    the specified file. The portioned data will be needed in the second task for parallel
    processing.
    '''

    if '.' not in path.split('/')[-1]:
        return 'PATH MUST BE A FILE'
    path = preprocess_path(path.replace('.', '-'))

    if r.get(f'{FB_BASE_URL}{path}.json').text == 'null':
        return 'FILE DOES NOT EXIST'

    data = json.loads(r.get(f'{FB_BASE_URL}{partition.replace("-", "/")}.json').text)
    if not data:
        return 'INVALID PARTITION'
    else:
        if type(data) == dict:
            data = list(data.values())
        elif type(data) == list:
            data = list(filter(lambda x: x is not None, data))
    output = ', '.join(data[0].keys())
    for d in data:
        output = '\n'.join([output, ', '.join([str(v) for v in d.values()])])
    return output
