import pandas as pd
import numpy as np

import pmr.html_templates as funct_data


def map_fun_4(imp: str, params: list) -> list:
    # 'SELECT gt.Mean, gc.`Mean cumulative mass balance`</br>FROM glaciers_csv.csv gc
    # INNER JOIN global_temp.csv gt </br>ON gc.Year = gt.Year'
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    output = []

    partitions = com.getPartitionLocations('/datasets/glaciers_csv.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/glaciers_csv.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df = df.astype({'Year': int, 'Mean cumulative mass balance': float})
        output.append([(i[1]['Year'], ('GC', i[1]['Mean cumulative mass balance'])) for i in df.iterrows()])
    
    partitions = com.getPartitionLocations('/datasets/global_temp.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/global_temp.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df = df.astype({'Year': int, 'Mean': float})
        df = df[df['Source'] == 'GISTEMP']
        output.append([(i[1]['Year'], ('GT', i[1]['Mean'])) for i in df.iterrows()])
    
    return output


def reduce_fun_4(map_res: list, params: list) -> str:
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    res = {}
    for i, j in flat_list:
        if i not in res.keys():
            res[i] = []
        res[i].append(j)

    output = []
    for i, j in res.items():
        if len(j) < 2:
            continue
        else:
            for k in j:
                if k[0] == 'GC':
                    for l in j:
                        if l[0] == 'GT':
                            output.append(str((l[1], k[1])))

    return output


# 'SELECT MONTH(co.Date) AS Month, AVG(co.), co.Average</br>FROM c.o2_ppm.csv co</br>GROUP BY MONTH(co.date);'
def map_fun_3(imp: str, params: list) -> list:
    # 'SELECT MONTH(co.Date) AS Month, AVG(co.), co.Average</br>FROM c.o2_ppm.csv co</br>GROUP BY MONTH(co.date);'
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    output = []

    partitions = com.getPartitionLocations('/datasets/co2_ppm.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/co2_ppm.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df[['Year', 'Month', 'Day']] = df['Date'].str.split('-', expand = True)
        df = df.astype({'Average': float, 'Month': int})
        output.append([(i[1]['Month'], i[1]['Average']) for i in df.iterrows()])

    return output


def reduce_fun_3(map_res: list, params: list) -> str:
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    months = {i:[] for i in range(1, 13)}
    for i, j in flat_list:
        months[i].append(j)
    output = [str((i, round(np.average(j), 2))) for i, j in months.items()]

    return [output]


def map_fun_2(imp: str, params: list) -> list:
    # 'SELECT year</br>FROM co2_ppm.csv</br>WHERE average >= lower AND average <= upper;',
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    output = []

    partitions = com.getPartitionLocations('/datasets/co2_ppm.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/co2_ppm.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df[['Year', 'Month', 'Day']] = df['Date'].str.split('-', expand = True)
        df = df.astype({'Average': float, 'Year': int})

        part_result = df[(df['Average'] >= float(params[0])) & (df['Average'] <= float(params[1]))].Date.to_list()
        output.append([(i, 1) for i in part_result])

    return output


def reduce_fun_2(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    output = len(flat_list)

    return output


def map_fun_1(imp: str, params: list) -> list:
    # 'SELECT GMSL FROM global_mean_sea_level.csv WHERE month = month AND year = year;',
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    output = []

    partitions = com.getPartitionLocations('/datasets/global_mean_sea_level.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/global_mean_sea_level.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df[['Year', 'Month', 'Day']] = df['Time'].str.split('-', expand = True)
        df = df.astype({'GMSL': float, 'GMSL uncertainty': float, 'Year': int, 'Month': int})
        part_result = df[(df['Month'] == int(params[0])) & (df['Year'] == int(params[1]))].GMSL.to_list()
        
        output.append([(i, 1) for i in part_result])
        
    return output


def reduce_fun_1(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    output = ' '.join(np.unique([str(i) for i in flat_list]))

    return output


def map_fun_0(imp: str, params: list) -> list:
    # 'SELECT year FROM fossil_fuels.csv WHERE total >= lower AND total <= upper;'
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    output = []

    partitions = com.getPartitionLocations('/datasets/fossil_fuels.csv').split('\n')
    for p in partitions:
        part_data = com.readPartition('/datasets/fossil_fuels.csv', p)

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df = df.astype({'Total': int, 'Year': int, 'Per Capita': float})
        part_result = df[(df['Total'] >= int(params[0])) & (df['Total'] <= int(params[1]))].Year.to_list()

        output.append([(i, 1) for i in part_result])

    return output


def reduce_fun_0(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    output = ' '.join(np.unique([str(i) for i in flat_list]))

    return output


funct_guide = [(map_fun_0, reduce_fun_0), (map_fun_1, reduce_fun_1), (map_fun_2, reduce_fun_2), (map_fun_3, reduce_fun_3), (map_fun_4, reduce_fun_4)]


def pmr_wrapper(imp: str, funct_id: int, params: list):
    map_res = funct_guide[funct_id][0](imp, params)
    red_res = funct_guide[funct_id][1](map_res, params)

    return map_res, red_res


def call_funct(data: dict):
    imp = data['imp']
    funct_id = int(data['funct'])
    params = data['params'].split('\n')

    map_res, red_res = pmr_wrapper(imp, funct_id, params)
    return {'map_res': ', '.join([str(i) for i in map_res]), 'final_res': red_res}


def sel_funct(funct_id: str):
    try:
        funct_id = int(funct_id)
        explanation_header = funct_data.exp_headers[funct_id]
        explanation_body = funct_data.exp_bodies[funct_id]
        query_form = funct_data.funct_forms[funct_id]
    except Exception as e:
        explanation_header = "BAD_FUNCT_ID"
        explanation_body = 'NO TEMPLATE DATA AVAILABLE'
        query_form = ''

    return {'exp_header': explanation_header, 'exp_body': explanation_body, 'query_form': query_form}
