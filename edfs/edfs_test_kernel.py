def get_implementation():
    print()
    implementation = input('Enter Integer to Choose Implementation to Test:\n1 - firebase\n2 - mongodb\n3 - mysql\n>>> ')
    print()

    if implementation == '1':
        import firebase.commands as com
    elif implementation == '2':
        import mongodb.commands as com
    elif implementation == '3':
        import mysql.commands as com
    else:
        print('INVALID INPUT')
        exit()

    return com


def main():
    com = get_implementation()

    print('KERNEL STARTED, ENTER "QUIT" TO EXIT\n')

    inp = ''
    while True:
        inp = input('>>> ')
        split = inp.split()
        if len(split) == 0:
            continue

        call = split[0]
        if call == 'mkdir':
            if len(split) != 2:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.mkdir(split[1]))
        elif call == 'ls':
            if len(split) != 2:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.ls(split[1]))
        elif call == 'cat':
            if len(split) != 2:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.cat(split[1]))
        elif call == 'rm':
            if len(split) != 2:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.rm(split[1]))
        elif call == 'put':
            if len(split) != 4:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.put(split[1], split[2], split[3]))
        elif call == 'getPartitionLocations':
            if len(split) != 2:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.getPartitionLocations(split[1]))
        elif call == 'readPartition':
            if len(split) != 3:
                print('Incorrect Number of Arguments')
                continue
            else:
                print(com.readPartition(split[1], split[2]))
        elif inp.upper() == 'QUIT':
            print('Exiting.')
            break
        else:
            print('Invalid Command.')


if __name__ == '__main__':
    main()
