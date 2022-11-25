import pmr.html_templates as funct_data


def map_fun_0(imp: str, params: [lower, upper]) -> list:

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
        df = df.astype({'Total': 'int64', 'Year': 'int64', 'per capita': 'float64'})
        part_result = list(c[(c['Total'] >= params[0]) & (c['Total'] <= params[1])].Year.values[:])

        output += part_result 

    return output


def reduce_fun_0(map_res: list, params: list) -> str:
    
    # REDUCE RESULTS FROM MAP HERE
    output = ' '.join(np.unique(map_res))

    return output


funct_guide = [(map_fun_0, reduce_fun_0)]


def pmr_wrapper(imp: str, funct_id: int, params: list):
    map_res = funct_guide[funct_id][0](imp, params)
    red_res = funct_guide[funct_id][1](map_res, params)

    return map_res, red_res


def call_funct(data: dict):
    imp = data['imp']
    funct_id = int(data['funct'])
    params = data['params'].split('\n')

    map_res, red_res = pmr_wrapper(imp, funct_id, params)
    return {'map_res': map_res, 'final_res': red_res}


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
