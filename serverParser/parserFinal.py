import json
import re

import moz_sql_parser
from moz_sql_parser import parse
import requests
from ImportantConfig import Config
from sql2fea import Sql2Vec
config = Config()
sql2vec = Sql2Vec()
BASE_URL = 'http://127.0.0.1:8000'
headers = {'content-type': 'application/json'}
alias_name_2_table_name = {
 'a1' : 'aka_name' ,
    'chn': 'char_name', 'ci': 'cast_info', 'cn': 'company_name', 'cn1': 'company_name',
                           'cn2': 'company_name', 'ct': 'company_type', 'mc': 'movie_companies',
                           'mc1': 'movie_companies', 'mc2': 'movie_companies', 'rt': 'role_type', 't': 'title',
                           't1': 'title', 't2': 'title', 'k': 'keyword', 'lt': 'link_type', 'mk': 'movie_keyword',
                           'ml': 'movie_link', 'mi': 'movie_info', 'mi_idx': 'movie_info_idx',
                           'mi_idx2': 'movie_info_idx', 'mi_idx1': 'movie_info_idx', 'miidx': 'movie_info_idx',
                           'kt': 'kind_type', 'kt1': 'kind_type', 'kt2': 'kind_type', 'at': 'aka_title',
                           'an': 'aka_name', 'an1': 'aka_name', 'cc': 'complete_cast', 'cct1': 'comp_cast_type',
                           'cct2': 'comp_cast_type', 'it': 'info_type', 'it1': 'info_type', 'it2': 'info_type','it3': 'info_type','pi': 'person_info', 'n1': 'name', 'n': 'name'}

operator_map = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "lt": "<",
    "gte": ">=",
    "lte": "<=",
    "like": "LIKE",
    "not_like": "NOT LIKE",
    "in": "IN",
    "between": "BETWEEN",
    "or": "OR",
    "and": "AND"
}  # to be filled with other possible values


def get_table_id(table_name):
    url = f'{BASE_URL}/tables/{table_name}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['table_id']
    else:
        return None


def get_attribute_id(attribute_name, table_name):
    url = f'{BASE_URL}/tables/{table_name}/attributes/{attribute_name}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['attribute_id']
    else:
        return None


def update_query_join_order(query_id, join_order):
    url = f'{BASE_URL}/queries/updateJoinOrder'
    data = {'queryId': query_id, 'joinOrder': join_order}
    response = requests.put(url, data=json.dumps(data), headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def create_query(query,results):
    print('the latest query',query)
    json_object = parse_sql_query(query,result=results)
    print(json.dumps(json_object,indent=4))
    response = requests.post(BASE_URL + '/queries/', data=json.dumps(json_object), headers=headers)

    if response.status_code == 200:
        query_id = response.json()['id']
        return query_id
    else:
        return None
def parse_sql_query(sql_query, result=[]):
    parsed_query = parse(sql_query)

    # Extract necessary elements from the parsed query
    parsed_select = parsed_query['select']
    parsed_tables = parsed_query['from']
    tables = {}
    where_clause = parsed_query.get('where')
    join_clause = parsed_query.get('join')
    # Create the JSON object
    list_join = result[0][:-1].split(',')
    print(list_join)
    string_join1 = [config.id2aliasname[int(x)] for x in list_join]
    list_join = result[1][:-1].split(',')
    string_join2 = [config.id2aliasname[int(x)] for x in list_join]
    if len(result)> 0:
        json_object = {
        "query": sql_query,
        "table": [],
        "selection": [],
        "projection": [],
        "join": [],
        "execution_time_hybride": result[5],
        "execution_time_pg": result[6],
        "estimated_execution_time1":result[2],
        "estimated_execution_time2":result[3] ,
        "join_order1":str(string_join1),
        "join_order2":str(string_join2),
        "choosed_plan":result[4] ,
        "prefix":result[7],
    }
    else:
        json_object = {
            "query": sql_query,
            "table": [],
            "selection": [],
            "projection": [],
            "join": [],
          #  "execution_time_pg": result[6],
          # "estimated_execution_time2": result[3],
           # "join_order1": result[0],
           # "join_order2": result[1],
           # "choosed_plan": result[4],
            # "prefix": result[7],
        }
    # Add table IDs to the JSON object
    for table in parsed_tables:
        table_id = get_table_id(table['value'])
        if table_id:
            json_object['table'].append(table_id)

    # Add projections to the JSON object
    projections, projection_aliases = get_query_projections(parsed_select)
    for index, value in enumerate(projections):
        if not value['all']:
            value['alias'] = projection_aliases[index]
        else:
            value['alias'] = ''
        json_object['projection'].append(value)
    # Add joins to the JSON object
    joins, selection_conditions = get_join_conditions(parsed_query)
    json_object['join'] = joins

    # Add selections to the JSON
    selection = get_selections(parsed_query, selection_conditions)
    json_object['selection'] = selection

    return json_object


def get_query_projections(parsed_select):
    projection_aliases = []
    projections = []
    if len(parsed_select) > 2:
        for column in parsed_select:
            projection = {}
            for prop, value in column.items():
                if isinstance(value, dict):
                    key_list = list(value.keys())
                    key = key_list[0]
                    projection['all'] = False
                    projection['aggregation'] = key
                    projection['projection'] = f"{key} ({value[key]})"
                    projection['attribute_id'] = get_attribute_id(value[key].split('.')[1],
                                                                  alias_name_2_table_name[value[key].split('.')[0]])
                    projections.append(projection)
                if isinstance(value, str):
                    projection_aliases.append(value)
    else:
        if not isinstance(parsed_select,list):
          parsed_select = parsed_select.items()
        for prop, value in parsed_select:
            projection = {}
            if isinstance(value, dict):
                key_list = list(value.keys())
                key = key_list[0]

                if key == 'count':
                   projection['all'] = True
                   projection['aggregation'] = key
                   projection['projection'] = f"{key}(*)"
                   projection['attribute_id'] =None
                else:
                    projection['all'] = False
                    projection['aggregation'] = key
                    projection['projection'] = f"{key} ({value[key]})"
                    projection['attribute_id'] = get_attribute_id(value[key].split('.')[1],
                                                                  alias_name_2_table_name[value[key].split('.')[0]])
                projections.append(projection)
            if isinstance(value, str):
                projection_aliases.append(value)
    return projections, projection_aliases

def get_join_conditions(parsed_query):
    table_aliases = [table['name'] for table in parsed_query['from']]

    join_conditions = parsed_query['where']['and']

    joins = []
    selection_conditions = []
    for condition in join_conditions:
        join = {}
        join_attributes = []
        left_join_attribute = {}
        right_join_attribute = {}
        if 'eq' in condition and isinstance(condition['eq'][0], str) and isinstance(condition['eq'][1], str) and '.' in \
                condition['eq'][0] and '.' in condition['eq'][1]:

            left = condition['eq'][0].split('.')[0]
            right = condition['eq'][1].split('.')[0]
            if left in table_aliases and right in table_aliases:
                join["join"] = f"{condition['eq'][0]} = {condition['eq'][1]}"
                left_join_attribute["attribute_id"] = get_attribute_id(condition['eq'][0].split('.')[1],
                                                                       alias_name_2_table_name[
                                                                           condition['eq'][0].split('.')[0]])
                left_join_attribute["position"] = 1
                join_attributes.append(left_join_attribute)
                right_join_attribute["attribute_id"] = get_attribute_id(condition['eq'][1].split('.')[1],
                                                                        alias_name_2_table_name[
                                                                            condition['eq'][1].split('.')[0]])
                right_join_attribute["position"] = 2
                join_attributes.append(right_join_attribute)

                join["join_attributes"] = join_attributes
            joins.append(join)
        else:
            selection_conditions.append(condition)

    return joins, selection_conditions


def get_selections(parsed_query, selection_conditions):
    parsed_query['where']['and'] = selection_conditions
    new_query_with_updated_where = moz_sql_parser.format(parsed_query)
    idx = new_query_with_updated_where.index("WHERE")
    where_clause = new_query_with_updated_where[idx:]
    where_clause = where_clause.replace("<>", "!=")

    # Define a regular expression pattern to extract the attribute name and comparison operator
    pattern = r'(?P<attribute>\w+\.\w+)\s+(?P<operator>=|!=|>|<|LIKE|NOT LIKE|IN|BETWEEN)\s+(?P<value>\'.?\'|\(.?\)|\d+)'

    # Split the WHERE clause statement into individual selection statements
    selection_statements = re.split(r'\s+(?:AND)\s+', where_clause.replace('WHERE', ''), flags=re.IGNORECASE)

    # Initialize an empty list to hold the selection objects
    selection = []

    # Loop through each selection statement
    for statement in selection_statements:
        # Initialize an empty list to hold the operators for this selection
        operators = []

        # Extract the attribute name, operator, and value for each operator in the selection
        for match in re.finditer(pattern, statement):
            # Extract the attribute name, operator, and value
            attribute = match.group('attribute')
            operator = match.group('operator')
            value = match.group('value')

            # If the value is a string, remove the quotation marks
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]

            # Map the attribute name to an attribute ID
            attribute_id = get_attribute_id(attribute.split('.')[1], alias_name_2_table_name[attribute.split('.')[0]])
            # Add the operator and value to the list for this selection
            operators.append({
                'operator': operator,
                'value': value,
                'domain_id': 1
            })

        # Assemble the selection object for this selection statement
            selection.append({
            'selection': statement,
            'attribute_id': attribute_id,
            'operators': operators
            })
            return selection