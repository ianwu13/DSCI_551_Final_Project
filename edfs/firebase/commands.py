import requests as r
import json

from creds import FB_BASE_URL


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
    if path[-1] == '/':
        path = path[:-1]
    if path[:6] == '/root/':
        path = path[6:]
    elif path[0] == '/':
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
def ls(path: str='.'): 
    # listing content of a given directory, e.g., ls /user
    pass


def cat(path: str): 
    # display content of a file, e.g., cat /user/john/hello.txt
    path = preprocess_path(path.replace('.', '-'))

    partitions = json.loads(r.get(f'{FB_BASE_URL}{path}.json').text)
    data = []
    for p in partitions:
        data.append(r.get(f'{FB_BASE_URL}{partitions[p].replace("-", "/")}.json').text.replace('"', '').split('\\n'))

    output = ''
    for i in range(len(data[0])):
        for j in range(len(data)):
            try:
                line = data[j][i]
            except:
                return output
            output = ''.join([output, f'{line}\n'])


def rm(path: str): 
    # remove a file from the file system, e.g., rm /user/john/hello.txt
    pass


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
    destination_path = f'{preprocess_path(destination_path)}/{f_name}'

    # Check if file already exists
    if check_exists(f'{FB_BASE_URL}{destination_path}.json'):
        return 'FILE ALREADY EXISTS IN EDFS'
    else:
        try:
            f = open(file_path, 'r')
        except:
            return 'INVALID FILE PATH'
        lines = f.readlines()

        if len(lines) < k:
            k = len(lines)
        cur_datanode = 1
        for p in range(k):
            datanode = f'datanode_{cur_datanode}'
            dn_b_num = get_next_b_num(datanode)
            r.put(f'{FB_BASE_URL}{destination_path}/p{p+1}.json', f'"{datanode}-{dn_b_num}"')

            data = lines[p].replace('"', '').replace('\n', '')
            for i in range(p+k, len(lines), k):
                data = '\\n'.join([data, lines[i].replace('"', '').replace('\n', '')])

            r.put(f'{FB_BASE_URL}{datanode}/{dn_b_num}.json', f'"{data}"')

            cur_datanode = get_next_datanode(cur_datanode)

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
