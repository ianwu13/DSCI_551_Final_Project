IMPS = ['MONGODB', 'FIREBASE', 'MYSQL']
FILES = ['data/co2_ppm.csv', 'data/fossil_fuels.csv', 'data/glaciers_csv.csv', 'data/global_mean_sea_level.csv', 'data/global_temp.csv']


def upload_implementation(imp: str):
    if imp == 'FIREBASE':
        from edfs.firebase.commands import mkdir, put
    elif imp == 'MYSQL':
        from edfs.mysql.commands import mkdir, put
    elif imp == 'MONGODB':
        from edfs.mongodb.commands import mkdir, put
    else:
        raise ValueError('INVALID IMPLEMNENTATION SPECIFIED')

    print(mkdir('datasets'))
    for f in FILES:
        print(put(f, 'datasets/', 2))


def main():
    for imp in IMPS:
        print(imp)
        upload_implementation(imp)


if __name__ == '__main__':
    main()
