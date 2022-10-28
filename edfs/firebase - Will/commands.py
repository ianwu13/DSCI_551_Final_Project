def mkdir(path: str): 
    # create a directory in file system, e.g., mkdir /user/john
    pass


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
    pass


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
