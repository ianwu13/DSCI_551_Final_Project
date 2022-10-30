import pymongo
from pymongo import MongoClient

from creds import FB_BASE_URL

client = MongoClient('localhost', 27017)
db = client['test']


def mkdir(path:str):
    
    # need to add: check if the directory exist and add to existing record if so

    if '.' in path:
        return ('INVALID PATH')
    else:
        path = preprocess_path(path)

    path_split = [s for s in path.split('/') if s != '']
    
    # collection - namenode
    collection = db[path_split[0]]
    
    # create entries for directories      
    namenode_dict = {'root': {}}
    cur_dict = namenode_dict
    for s in path.split('/')[1:]:
        if s in cur_dict.keys():
            cur_dict = cur_dict[s]
        else:
            cur_dict[s] = {}
            cur_dict = cur_dict[s]
    
    # check if path exist: (i). already exist (ii). partly exist, need to insert new data (iii). not exist
    path_exist = False
    for document in cursor:
        count = -1
        cur_dict = document
        for s in path_split[1:]:
            if path_exist:
                break
            if s in cur_dict:
                count += 1
                cur_dict = cur_dict[s]
                
                # for (i)
                if count == len(path_split[2:]):
                    path_exist = True
                    print('ALREADY EXIST')
                    break
    
    return collection.insert_one(namenode_dict)


def ls(path: str): 
    
    # listing content of a given directory, e.g., ls /user

    if '.' in path:
        return 'NOT A DIRECTORY'
    
    path = preprocess_path(path)
    path_split = [s for s in path.split('/') if s != '']
    collection = db[path_split[0]]
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
            return '\n'.join(keys)
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
