import xmltodict

import re

from collections import OrderedDict

import sys

import time

import os

from pathlib import Path

 

 

SELECT_STR = 'SELECT '

GROUP_STR = 'GROUP BY ALL'

AGG_BEHAV_KEY = '@aggregationBehavior'

FILTER_EXP_KEY = 'filterExpression'

ELEM_FILTER_KEY = 'elementFilter'

LAYOUT_KEY = 'layout'

ELEM_KEY = 'element'

INPUT_KEY = 'input'

MAPPING_KEY = 'mapping'

NAME_KEY = '@name'

TEXT_KEY = '#text'

VIEWNODE_KEY = 'viewNode'

ENTITY_KEY = 'entity'

LEFTINP_KEY = '@leftInput'

RIGHTINP_KEY = '@rightInput'

JOINTYP_KEY = '@joinType'

SRC_KEY = '@sourceName'

TRGT_KEY = '@targetName'

CALC_KEY = 'calculationDefinition'

FORMULA_KEY = 'formula'

YCOORD_KEY = '@yCoordinate'

XSITYP_KEY = '@xsi:type'

 

all_queries = dict()

join_node_tbl_alias =dict()

 

def has_inline_type(elem):

    return 'inlineType' in elem

 

def is_num_type(elem):

    if not has_inline_type(elem):

        return False

    return elem['inlineType']['@primitiveType'] in ['NUMBER','INTEGER','DECIMAL','FLOAT','DOUBLE','REAL']

 

def cast_type(elem):

    val = elem[CALC_KEY][FORMULA_KEY]

    datatype = elem['inlineType']['@primitiveType']

       

    if datatype =='DATE' and len(re.findall('date[\r\n\s]*\(.*?\)',val, flags=re.IGNORECASE)) == 0:

        val = 'try_to_date(' + val.replace('+',' || ') + ", 'yyyyMMdd')"

    elif datatype == 'TIMESTAMP':

        val = 'TO_TIMESTAMP_NTZ(' + val.replace('+',' || ') + ", 'yyyyMMddHHmmss')"

 

    return val

   

def is_list(obj):

    return isinstance(obj,list)

 

#TO ADD NEWLINE IN A LONG QUERY

def newline_beatify(s):

    return re.sub("((?:[^,]*,){5})", \\1\n, s, 0, re.DOTALL)

 

#TO CHECK IF THE COL IS AGGREGATED

def is_elem_aggregated(elem):

    if  AGG_BEHAV_KEY not in elem or elem[AGG_BEHAV_KEY]=='NONE' or elem[AGG_BEHAV_KEY]==FORMULA_KEY:

        return False

    else:

        return True

   

#TO REPLACE KEYS WITH VALUES IN A STRING USING A DICT

def replace_dict(query, rep_dict):

    mod_str = str(query)

    for repl, value in rep_dict.items():

        mod_str = mod_str.replace(repl, value)

 

    return mod_str

 

#CONVERTING SAP FUNCTIONS TO SNOWFLAKE FUNCTIONS

def repl_sap_func(s):

 

    #REPLACE SAP NOW() with CURRENT_TIMESTAMP

    matches = re.findall('now\(.*?\)',s, flags=re.IGNORECASE)

    match_dict= { item : "CURRENT_TIMESTAMP"   for item in matches  }

    mod_str = replace_dict(s, match_dict)

   

    #REPLACE isNull with IS NULL

    matches = re.findall('isnull[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : re.findall(r'\((.*?)\)', item)[0] + ' IS NULL'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

   

 

    #REPLACE IF with IFF

    matches = re.findall('IF[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'IFF('+re.findall(r'\((.*?)\)', item)[0] + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict).replace('if(','IFF(').replace('IF(','IFF(')

 

    #REPLACE CASE with DECODE

    matches = re.findall('case[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'DECODE('+re.findall(r'\((.*?)\)', item)[0] + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict).replace('case(','DECODE(')

 

    #REPLACE MIDSTR with SUBSTRING

    matches = re.findall('midstr[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'SUBSTRING('+re.findall(r'\((.*?)\)', item)[0] + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE LEFTSTR with LEFT

    matches = re.findall('leftstr[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'LEFT('+re.findall(r'\((.*?)\)', item)[0] + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE RIGHTSTR with RIGHT

    matches = re.findall('rightstr[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'RIGHT('+re.findall(r'\((.*?)\)', item)[0] + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE MATCH with REGEXP_LIKE

    matches = re.findall('match[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : 'REGEXP_LIKE('+re.findall(r'\((.*?)\)', item)[0].replace('*','.*') + ')'  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE SAP IN() with SNOWFLAKE IN

    matches = re.findall('in[\r\n\s]*\([\r\n\s]*".*".*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : re.findall(r'\((.*?)\)', item)[0].split(',')[0] + ' in (' +

              ', '.join(re.findall(r'\((.*?)\)', item)[0].split(',')[1:]) + ' )'

               for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE SAP DATE() with DATE with format

    matches = re.findall('^date[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : "DATE(" + re.findall(r'\((.*?)\)', item)[0] + r",\'yyyyMMdd\')"  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE SAP DAYSBETWEEN() with DATE with format

    matches = re.findall('daysbetween[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : "DATEDIFF(DAY," + re.findall(r'\((.*?)\)', item)[0] + r",\'yyyyMMdd\')"  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE SAP addays() with dateadd

    # matches = re.findall('adddays[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    # print(mod_str)

    # match_dict= { item : "DATEADD(DAY," + re.findall(r'\((.*?)\)', item)[0].split(',')[1] + ',' +

    #              re.findall(r'\((.*?)\)', item)[0].split(',')[0]  for item in matches  }

    # mod_str = replace_dict(mod_str, match_dict)

 

    #REPLACE SAP FORMAT() with TO_VARCHAR

    matches = re.findall('format[\r\n\s]*\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : "TO_VARCHAR(" + re.findall(r'\((.*?)\)', item)[0] + ")"  for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

 

    #REPLACE SAP STRING() with ::STRING

    matches = re.findall('string\(.*?\)',mod_str, flags=re.IGNORECASE)

    match_dict= { item : re.findall(r'\((.*?)\)', item)[0] + '::STRING'   for item in matches  }

    mod_str = replace_dict(mod_str, match_dict)

 

    return mod_str

 

#TO GET WHERE CLAUSE FROM XML

def get_filter_exp(node):

        if FILTER_EXP_KEY in node.keys():

            return 'WHERE ' + node[FILTER_EXP_KEY][FORMULA_KEY]

        elif ELEM_FILTER_KEY in node.keys():

            elem_filter = node[ELEM_FILTER_KEY]

           

 

            if is_list(elem_filter):

                where_str = 'WHERE '

                for elem_f in elem_filter:

                    if elem_f['valueFilter'][XSITYP_KEY] =='Column:SingleValueFilter':

                        where_str = where_str + '\n' + '"'+ elem_f['@elementName'] + '"=\'' + elem_f['valueFilter']['@value']  +'\'\nAND'

                    elif elem_f['valueFilter'][XSITYP_KEY] =='Column:ListValueFilter':

                        operands = elem_f['valueFilter']['operands'] if is_list(elem_f['valueFilter']['operands']) else \

                        list(elem_f['valueFilter']['operands'])

                   

                        operands = ["'" + op['@value'] + "'" for op in operands ]

                        in_str = ' IN (' + ','.join(operands) + ')'

                        where_str = where_str + '\n' + '"' + elem_f['@elementName'] +'"' + in_str

                   

                return where_str

                

            else:

                if elem_filter['valueFilter'][XSITYP_KEY] =='Column:SingleValueFilter':

                    return 'WHERE ' +'"' + elem_filter['@elementName'] + '"=\'' + elem_filter['valueFilter']['@value']  +'\''

                elif elem_filter['valueFilter'][XSITYP_KEY] =='Column:ListValueFilter':

                    operands = elem_filter['valueFilter']['operands'] if is_list(elem_filter['valueFilter']['operands']) else \

                        list(elem_filter['valueFilter']['operands'])

                   

                    operands = ["'" + op['@value'] + "'" for op in operands ]

                    in_str = ' IN (' + ','.join(operands) + ')'

                    return 'WHERE ' +'"' + elem_filter['@elementName'] +'"' + in_str

 

                       

 

        else:

            return ''

                   

        

 

def get_obj(s):

    return s.split('/')[-1]

 

#TO GET FROM CLAUSE FROM XML

def get_from_part(node):

   

    from_part = "FROM " + get_obj(node[ENTITY_KEY] if ENTITY_KEY in node else node[VIEWNODE_KEY][TEXT_KEY])

    return from_part

 

 

def type_of_join(s):

    if 'leftouter' in s.lower():

        return ' LEFT JOIN'

    elif 'rightouter' in s.lower():

        return  ' RIGHT JOIN '

    elif 'inner' in s.lower() or 'referential' in s.lower():

        return ' INNER JOIN '

 

 

def get_cols_from_map(mp, reverse = True, fromnode=None):

 

    res= []

    if fromnode == 'JoinNode':

        if reverse:

            for k,v in mp.items():

                if v.split('.')[-1].strip().lower() == k.strip().lower():

                    res.append(v)

                else:

                    res.append(v + ' ' + k)

        else:

            for k,v in mp.items():

                if v.split('.')[-1].strip().lower() == k.strip().lower():

                    res.append(k)

                else:

                    res.append(k + ' ' + v)

 

    else:

        if reverse:

            for k,v in mp.items():

                if v.strip().lower() == k.strip().lower():

                    res.append(v)

                else:

                    res.append(v + ' ' + k)

        else:

            for k,v in mp.items():

                if v.strip().lower() == k.strip().lower():

                    res.append(k)

                else:

                    res.append(k + ' ' + v)

 

    return res

   

#TO GET ON PART OF A JOIN NODE

def generate_on_part(leftcols, rightcols, leftalias, rightalias):

    if is_list(leftcols):

        on_cols = [leftalias + '.'+ k +' = '+ rightalias + '.'+ v for k,v in zip(leftcols,rightcols)]

        on_part = 'ON '+ ' and\n'.join(on_cols)

 

    elif isinstance(leftcols, str):

        on_part= 'ON ' + leftalias + '.'+ leftcols + ' = ' + rightalias + '.'+ rightcols

 

    return on_part

 

 

#TO CHECK IF A COL IS CALCULATED

def is_calc_col(col):

    return CALC_KEY in col

 

 

#TO GET CALCULATED COLS FROM A NODE

def get_calc_columns(elem):

    mapping =dict()

    for x in elem :

        if has_inline_type(x):

            mapping[x[NAME_KEY]] = cast_type(x)  

        else:

            mapping[x[NAME_KEY]] = x[CALC_KEY][FORMULA_KEY].replace('+',' || ')

 

    return mapping

 

#WRAP AN AGGREGATED COL WITH THE AGG FUNC

def wrap_agg(s, fun):

    return fun + '(' + s + ') '

 

 

#GENERATES JOIN FOR SINGLE OR MULTIPLE JOINS

def generate_full_join(node, node_name):

 

 

    all_tbls_alias = dict()

    all_tbls_alias_repl =dict()

    all_tbls_alias_repl1 = dict()

    full_join = 'FROM\n'

 

    if is_list(node):

       

        i = 0

        for j in range(len(node)):

            i+=1

            all_tbls_alias.setdefault((node[j][LEFTINP_KEY] ) , 'T'+ str(i)  )

            i+=1

            all_tbls_alias.setdefault(node[j][RIGHTINP_KEY], 'T'+ str(i) )        

 

            #CHECKING IF ITS THE FIRST JOIN NODE, THEN WE APPEND LEFT AS WELL AS RIGHT TABLE IN QUERY

            if j == 0:

                join_part=  node[j][LEFTINP_KEY]+' ' + all_tbls_alias[node[j][LEFTINP_KEY]] +'\n' + type_of_join(node[j][JOINTYP_KEY]) + '\n' \

                        +  node[j][RIGHTINP_KEY]+' ' + all_tbls_alias[node[j][RIGHTINP_KEY]] + '\n'

            else:

                join_part =  '\n' + type_of_join(node[j][JOINTYP_KEY]) + '\n' \

                        +  node[j][RIGHTINP_KEY]+' ' + all_tbls_alias[node[j][RIGHTINP_KEY]] + '\n'

 

           

            on_part = generate_on_part(node[j]['leftElementName'], node[j]['rightElementName'],

                                             all_tbls_alias[node[j][LEFTINP_KEY]], all_tbls_alias[node[j][RIGHTINP_KEY]] )

           

 

            join_part = join_part + on_part

            full_join = full_join + join_part

 

        for k,v in all_tbls_alias.items():

            all_tbls_alias_repl[k] = get_obj(k)

            all_tbls_alias_repl1[get_obj(k)] = v

 

       

        full_join = replace_dict(full_join,all_tbls_alias_repl )

 

        join_node_tbl_alias.update({node_name : all_tbls_alias_repl1})

 

       

 

 

    else:

 

        leftalias = 'T1'

        rightalias = 'T2'

 

        all_tbls_alias.setdefault(node[LEFTINP_KEY], leftalias  )

        all_tbls_alias.setdefault(node[RIGHTINP_KEY], rightalias  )

 

        join_part=  node[LEFTINP_KEY]+' ' + leftalias +'\n' + type_of_join(node[JOINTYP_KEY]) + '\n'  + \

                        node[RIGHTINP_KEY]+' ' + rightalias + '\n'

            

        on_part = generate_on_part(node['leftElementName'], node['rightElementName'],leftalias, rightalias )

       

        join_part = join_part + on_part

        full_join = full_join + join_part

 

        for k,v in all_tbls_alias.items():

            all_tbls_alias_repl[k] = get_obj(k)

            all_tbls_alias_repl1[get_obj(k)] = v

 

        full_join = replace_dict(full_join, all_tbls_alias_repl )

 

    join_node_tbl_alias.update({node_name: all_tbls_alias_repl1 })

 

 

    return full_join

 

 

#TO GET COL MAPPING IN A JOIN NODE

def get_other_colmap(nodes , othercols, node_name) -> dict:

 

    join_nodes = [x for x in nodes[INPUT_KEY] if MAPPING_KEY in x]

    node_aliases = join_node_tbl_alias[node_name]

 

    all_join_nodes =dict()

    src_trgt_map = dict()

    for item in join_nodes :

        if VIEWNODE_KEY in item and is_list(item[MAPPING_KEY]  ):

            for x in item[MAPPING_KEY]:

                all_join_nodes.setdefault(x[TRGT_KEY], node_aliases[get_obj(item[VIEWNODE_KEY][TEXT_KEY])] +'.' + x[SRC_KEY])

              

        elif VIEWNODE_KEY in item and not is_list(item[MAPPING_KEY]  ):

            all_join_nodes.setdefault(item[MAPPING_KEY][TRGT_KEY],

                                      node_aliases[get_obj(item[VIEWNODE_KEY][TEXT_KEY])] +'.' + item[MAPPING_KEY][SRC_KEY])

            

 

           

        elif ENTITY_KEY in item and is_list(item[MAPPING_KEY]  ):

            for x in item[MAPPING_KEY]:

                all_join_nodes.setdefault(x[TRGT_KEY], node_aliases[get_obj(item[ENTITY_KEY])] +'.' + x[SRC_KEY])

 

        elif ENTITY_KEY in item and not is_list(item[MAPPING_KEY]  ):

            all_join_nodes.setdefault(item[MAPPING_KEY][TRGT_KEY],

                                      node_aliases[get_obj(item[ENTITY_KEY])] +'.' + item[MAPPING_KEY][SRC_KEY])

 

    for col in othercols:

        if is_elem_aggregated(col):

            src_trgt_map.setdefault(col[NAME_KEY],   wrap_agg(all_join_nodes[col[NAME_KEY]] , col[AGG_BEHAV_KEY]  )   )

        else:

            src_trgt_map.setdefault(col[NAME_KEY],  all_join_nodes[col[NAME_KEY]])

 

 

    return src_trgt_map

 

def filter_join_nodes(all_nodes):

    join_nodes = list(filter(lambda x: x[XSITYP_KEY] == 'View:JoinNode' ,all_nodes))

    join_nodes_final = []

   

    for node in join_nodes:

        if is_list(node['join']):

            if any([JOINTYP_KEY in join  for join in node['join'] ]):

                join_list = list(filter(lambda x: JOINTYP_KEY in x   , node['join'] ))

                node['join'] = join_list

                join_nodes_final.append(node)

        else:

            if JOINTYP_KEY in node['join']:

                join_nodes_final.append(node)

   

    return join_nodes_final

 

   

 

#EXTRACTS PROJECTION NODE QUERIES

def projection_qry_extract(projection_nodes):

   

    queries= dict()

    for node in projection_nodes:

       

        from_part = get_from_part(node[INPUT_KEY])

        where_part = get_filter_exp(node)

        from_with_filter = from_part + '\n' +  where_part

 

        col_dict = {x[TRGT_KEY]: x[SRC_KEY] for x in node[INPUT_KEY][MAPPING_KEY] }

        columns = get_cols_from_map(col_dict) #   [ x[SRC_KEY]+ ' ' + x[TRGT_KEY] for x in node[INPUT_KEY][MAPPING_KEY]  ]

        calc_col_map = get_calc_columns( [ x for x in node[ELEM_KEY] if is_calc_col(x)  ] )

        calc_cols = get_cols_from_map(calc_col_map)    

        columns.extend(calc_cols)

       

        select_part = SELECT_STR + ', '.join(columns) + "\n"

        full_query = newline_beatify(repl_sap_func(select_part + from_with_filter))

      

        queries[node[NAME_KEY]] = {'query':full_query, 'pos': int(node[LAYOUT_KEY][YCOORD_KEY])    }

 

    all_queries.update(queries)

 

#EXTRACTS AGGREGATION NODE QUERIES

def aggregation_qry_extract(aggregation_nodes):

    queries = dict()

 

    for node in aggregation_nodes:

       

        from_part = get_from_part(node[INPUT_KEY])

        where_part = get_filter_exp(node)

        from_with_filter = from_part +'\n' + where_part

 

        final_cols = dict()   

        group_by_part = GROUP_STR

       

 

        source_target_mapping = dict()

 

        if not isinstance(node[INPUT_KEY][MAPPING_KEY],list):

            source_target_mapping.update({ node[INPUT_KEY][MAPPING_KEY][TRGT_KEY]: node[INPUT_KEY][MAPPING_KEY][SRC_KEY]})

        else:

            source_target_mapping.update({ x[TRGT_KEY]: x[SRC_KEY] for x in node[INPUT_KEY][MAPPING_KEY] })   

 

   

        if  isinstance(node[ELEM_KEY],list):

            for x in node[ELEM_KEY]:

 

                if x[NAME_KEY] in source_target_mapping:

                    final_cols[x[NAME_KEY]] = source_target_mapping[x[NAME_KEY]]

               

                else:

                    final_cols[x[NAME_KEY]] = x[CALC_KEY][FORMULA_KEY] if is_num_type(x) \

                                                else x[CALC_KEY][FORMULA_KEY].replace('+',' || ')

               

                final_cols[x[NAME_KEY]] = wrap_agg(final_cols[x[NAME_KEY]], x[AGG_BEHAV_KEY]) \

                if is_elem_aggregated(x) else final_cols[x[NAME_KEY]]

               

        else:

           

            final_cols[node[ELEM_KEY][NAME_KEY]] = source_target_mapping[node[ELEM_KEY][NAME_KEY]] \

                    if node[ELEM_KEY][NAME_KEY] in source_target_mapping else None

           

            final_cols[node[ELEM_KEY][NAME_KEY]] = wrap_agg(final_cols[node[ELEM_KEY][NAME_KEY]], node[ELEM_KEY][AGG_BEHAV_KEY])\

                                                    if is_elem_aggregated(node[ELEM_KEY]) else final_cols[node[ELEM_KEY][NAME_KEY]]

           

 

        select_cols= get_cols_from_map(final_cols)

        select_part = SELECT_STR + ', '.join(select_cols)

        

        full_query = newline_beatify(repl_sap_func(select_part + '\n' + from_with_filter + '\n' + group_by_part))

        queries[node[NAME_KEY]] = {'query':full_query, 'pos': int(node[LAYOUT_KEY][YCOORD_KEY])    }

       

    all_queries.update(queries)

 

#EXTRACTS JOIN NODE QUERIES

def join_qry_extract(join_nodes):

    queries = dict()

 

    for node in join_nodes:

       

        agg_elements = set([ x[AGG_BEHAV_KEY] for x in node[ELEM_KEY] if is_elem_aggregated(x)] )

        where_part = get_filter_exp(node)

 

        group_by_part = ''

 

        if any(agg_elements):

            group_by_part =  GROUP_STR

 

       

        full_join = generate_full_join(node['join'] , node_name= node[NAME_KEY])

        other_columns = [ x for x in node[ELEM_KEY]  if not is_calc_col(x) ]

        other_col_map = get_other_colmap(node, other_columns, node[NAME_KEY])

        calculated_colmap = get_calc_columns( [ x   for x in node[ELEM_KEY] if is_calc_col(x)  ] )

 

        source_target_mapping = dict()

        source_target_mapping.update(other_col_map)

        source_target_mapping.update(calculated_colmap)

 

        select_cols = get_cols_from_map(source_target_mapping, fromnode='JoinNode')

        select_qry = SELECT_STR + ', '.join(select_cols)

      

 

        full_query = repl_sap_func(select_qry + '\n' + full_join + '\n' + where_part + '\n' +group_by_part)

 

        other_col_map= {'"'+k+'"'   :v for k,v in other_col_map.items()}

 

        full_query= newline_beatify(replace_dict(query=full_query, rep_dict=other_col_map))

        queries[node[NAME_KEY]] = {'query':full_query, 'pos': int(node[LAYOUT_KEY][YCOORD_KEY])    }

       

    all_queries.update(queries)

 

#EXTRACTS RANK NODE QUERIES

def rank_qry_extract(rank_nodes):

    queries = dict()

 

    for node in rank_nodes:

       

        all_cols = [ x[NAME_KEY] for x in node[ELEM_KEY] ]

 

        from_part = get_from_part(node[INPUT_KEY])

       

 

        input_mapping = node[INPUT_KEY][MAPPING_KEY]

        source_target_mapping = { }

 

        if isinstance(input_mapping, list):

            source_target_mapping = { x[TRGT_KEY]: x[SRC_KEY]    for x in input_mapping  }

        else:

            source_target_mapping.update(    { input_mapping[TRGT_KEY]: input_mapping[SRC_KEY]  }   )

 

       

        partition = node['windowFunction']['partitionElement'].split('/')[-1]

        partitionby_part = 'PARTITION BY '+ partition

        order =  node['windowFunction']['order']

        rank_threshold= node['windowFunction']['rankThreshold']['constantValue']

        orderby_cols = []

        orderby_part = ' ORDER BY '

 

        if isinstance(order, list):

            orderby_cols = [ x['@byElement'].split('/')[-1] + ' ' + x['@direction']  for x in order]

        else:

            orderby_cols.append(  order['@byElement'].split('/')[-1] + ' ' + order['@direction']   )

 

        orderby_part = orderby_part + ', '.join(orderby_cols)

 

        rank_part = 'RANK() OVER ('+  partitionby_part+ orderby_part + ')'

        rank_col = list(filter(lambda x : x not in source_target_mapping,all_cols))[0]

 

        qualify_part = 'QUALIFY ' + rank_col + '= '+ rank_threshold

        from_with_filter = from_part + '\n' +  qualify_part

 

        source_target_mapping[rank_col] = rank_part

 

        all_cols = get_cols_from_map(source_target_mapping)

 

        select_part = SELECT_STR + ', '.join(all_cols)

        full_query = newline_beatify(repl_sap_func(select_part +'\n' + from_with_filter))

        queries[node[NAME_KEY]] = {'query': full_query, 'pos': int(node[LAYOUT_KEY][YCOORD_KEY])    }

 

    all_queries.update(queries)

 

#EXTRACTS UNION NODE QUERIES

def union_qry_extract(union_nodes):

    queries = dict()

 

    for node in union_nodes:

       

        from_objects = node[INPUT_KEY]

        from_mapping = []

        from_object_names = []

        select_qrys = []

        source_target_mapping = {}

       

        for i in range(len(from_objects)):

            from_object_names.append( get_from_part( from_objects[i]  ) )

            from_mapping= from_objects[i][MAPPING_KEY]

 

            for x in from_mapping:

                if x[XSITYP_KEY] == 'Type:ElementMapping':

                    source_target_mapping[x[TRGT_KEY]] = x[SRC_KEY]

                else:

                    source_target_mapping[x[TRGT_KEY]] = "NULL"

            

            select_part =  SELECT_STR + ', '.join( get_cols_from_map(source_target_mapping) )

           

            select_qrys.append(select_part)

 

   

        full_qrys = get_cols_from_map(dict(zip(from_object_names, select_qrys)) )

        full_query = newline_beatify( repl_sap_func('\n\nUNION ALL\n\n'.join(full_qrys)) )

       

        queries[node[NAME_KEY]] = {'query':full_query, 'pos': int(node[LAYOUT_KEY][YCOORD_KEY])    }

 

    all_queries.update(queries)

 

 

def main():

    Path("queries").mkdir(parents=True, exist_ok=True)

 

    for subdir, _ , files in os.walk('xmls/'):

        for file in files:

            XML_FILE = subdir + file

            SQL_FILE = 'queries/' + XML_FILE.split('/')[-1].split('.')[0]

 

            with open(XML_FILE, 'r', encoding='utf-8') as file:

                my_xml = file.read()

            

            all_nodes = dict(xmltodict.parse(my_xml) )['View:ColumnView'][VIEWNODE_KEY]

 

            projection_nodes = list(filter(lambda x: x[XSITYP_KEY] == 'View:Projection', all_nodes))

            aggregation_nodes = list(filter(lambda x: x[XSITYP_KEY] == 'View:Aggregation'     ,all_nodes))

            join_nodes = filter_join_nodes(all_nodes)

            rank_nodes =  list(filter(lambda x: x[XSITYP_KEY] == 'View:Rank'   ,all_nodes))

            union_nodes =  list(filter(lambda x: x[XSITYP_KEY] == 'View:Union'   ,all_nodes))

 

            projection_qry_extract(projection_nodes)

            aggregation_qry_extract(aggregation_nodes)

            join_qry_extract(join_nodes)

            rank_qry_extract(rank_nodes)

            union_qry_extract(union_nodes)

 

 

            all_queries_sorted = OrderedDict(sorted(all_queries.items(), key=lambda x: x[1]['pos'], reverse= True))

 

            original_stdout = sys.stdout

 

            with open(f'{SQL_FILE}.sql', 'w') as f:

                sys.stdout = f # Change the standard output to the file we created.

 

                print('with ')

 

                for k,v in all_queries_sorted.items():

                    print(k, ' as (')

                    print(v['query'].replace("\\'","'"))

                    print('),')

 

                sys.stdout = original_stdout # Reset the standard output to its original value

 

 

if __name__ == "__main__":

    start_time = time.time()

    main()

    print("Time taken (ms)=" ,round((time.time() - start_time)*1000))
