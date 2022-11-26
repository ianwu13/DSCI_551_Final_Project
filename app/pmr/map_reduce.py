import pandas as pd
import numpy as np

import pmr.html_templates as funct_data


# 'SELECT MONTH(co.Date) AS Month, AVG(co.), co.Average</br>FROM c.o2_ppm.csv co</br>GROUP BY MONTH(co.date);'
def map_fun_4(imp: str, params: list) -> list:
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

        temp_df = df.groupby('Month').Average.mean()
        part_result = [(temp_df.index[i], temp_df.iloc[i]) for i in range(len(temp_df))]
        output.append(part_result)

    return output


def reduce_fun_4(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    months = set([key for key, value in flat_list])

    temp = []
    for month in months:
        sum_, count = 0, 0
        for k,v in flat_list:
            if k == month:
                sum_ += v
                count += 1
        temp.append((month, sum_/count))
    output = ' '.join(np.unique([str(i) for i in temp]))

    return output


def map_fun_3(imp: str, params: list) -> list:
    # 'SELECT ff.Year, ff.`Gas Fuel`, ff.`Liquid Fuel`, ff.`Solid Fuel`, gt.Mean</br>
    # FROM fossil_fuels.csv ff</br>LEFT JOIN global_temp.csv gt ON ff.Year = gt.Year</br>WHERE gt.Mean >= temp;'
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'

    partitions = com.getPartitionLocations('/datasets/fossil_fuels.csv').split('\n')
    partitions2 = com.getPartitionLocations('/datasets/global_temp.csv').split('\n')
    fossil_df_list = []
    globaltemp_df_list = []
    for p in range(len(partitions)):
        part_data = com.readPartition('/datasets/fossil_fuels.csv', partitions[p])

        # MAP SPECIFIC PARTITON HERE
        names = part_data.split('\n')[0].split(',')
        names = [x.strip(' ') for x in names]
        data = [d.split(',') for d in part_data.split('\n')[1:]]
        data = [[d_.strip(' ') for d_ in d] for d in data]
        df = pd.DataFrame(data, columns=names)
        df = df.astype({'Year': int}) 
        fossil_df_list.append(df)

        part_data2 = com.readPartition('/datasets/global_temp.csv', partitions2[p])

        # MAP SPECIFIC PARTITON 2 HERE
        names2 = part_data2.split('\n')[0].split(',')
        names2 = [x.strip(' ') for x in names2]
        data2 = [d.split(',') for d in part_data2.split('\n')[1:]]
        data2 = [[d_.strip(' ') for d_ in d] for d in data2]
        df2 = pd.DataFrame(data2, columns=names2)
        df2 = df2.astype({'Mean': float, 'Year': int})
        df2 = df2[(df2['Mean'] >= float(params[0]))]
        globaltemp_df_list.append(df2)

    df = pd.concat(fossil_df_list)
    df2 = pd.concat(globaltemp_df_list)
    df3 = df.merge(df2, on='Year', how='left')

    output = []
    for i in range(len(df3)):
        year = ('Year', df3['Year'].values[i])
        gasFuel = ('Gas Fuel', df3['Gas Fuel'].values[i])
        liquidFuel = ('Liquid Fuel', df3['Liquid Fuel'].values[i])
        solidFuel = ('Solid Fuel', df3['Solid Fuel'].values[i])
        mean_ = ('Mean Global Temperature', df3['Mean'].values[i])
        temp_list = [year, gasFuel, liquidFuel, solidFuel, mean_]
        output.append(temp_list)
    
    return output


def reduce_fun_3(map_res: list, params: list) -> str:

    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    output = ' '.join(np.unique([str(i) for i in flat_list]))

    return output


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

        part_result = df[(df['Average'] >= float(params[0])) & (df['Average'] <= float(params[1]))].Year.to_list()
        output.append(part_result)

    return np.unique([np.array(o, dtype=float).tolist() for o in output])


def reduce_fun_2(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    flat_list = [item for sublist in map_res for item in sublist]
    output = ' '.join(np.unique([str(i) for i in flat_list]))

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
        
        output.append(part_result)
        
    return [np.array(o, dtype=int).tolist() for o in output]


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

        output.append(part_result)

    return [np.array(o, dtype=int).tolist() for o in output]


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
