import pymongo
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test']
collection = db['namenode']

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
    path_split = [s for s in path.split('/') if s != '']
    collection = db['namenode']
    cursor = collection.find({})
    
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


def put(file_path: str, destination_path: str, k: int): 
    '''
    uploading a file to file system, e.g., put(cars.csv, /user/john, k = # partitions) will
    upload a file cars.csv to the directory /user/john in EDFS. But note that the file
    should be stored in k partitions, and the file system should remember where the
    partitions are stored. You should design a method to partition the data. You may
    also have the user indicate the method, e.g., hashing on certain car attribute, in the
    put method. 
    '''
    pass


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
