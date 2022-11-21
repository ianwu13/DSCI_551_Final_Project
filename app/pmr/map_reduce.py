import pmr.html_templates as funct_data


def map_example(imp: str, params: list):
    if imp == 'firebase':
        import edfs.firebase.commands as com
    elif imp == 'mongo':
        import edfs.mongodb.commands as com
    elif imp == 'mysql':
        import edfs.mysql.commands as com
    else:
        return 'INVALID INPUT'
    
    # DO MAPPING HERE
    return f'Params: {params}'


def reduce_example(map_res: list, params: list):
    # REDUCE RESULTS FROM MAP HERE
    return 'EXAMPLE RESULT'


funct_guide = [(map_example, reduce_example)]


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
    except:
        explanation_header = "BAD_FUNCT_ID"
        explanation_body = 'NO TEMPLATE DATA AVAILABLE'
        query_form = ''

    return {'exp_header': explanation_header, 'exp_body': explanation_body, 'query_form': query_form}
