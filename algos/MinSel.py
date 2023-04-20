import moz_sql_parser
from algos.helper_functions import *
my_tables = {"aka_name": 0,
             "aka_title": 0,
             "cast_info": 0,
             "char_name": 0,
             "comp_cast_type": 0,
             "company_name": 0,
             "company_type": 0,
             "complete_cast": 0,
             "info_type": 0,
             "keyword": 0,
             "kind_type": 0,
             "link_type": 0,
             "movie_companies": 0,
             "movie_info_idx": 0,
             "movie_keyword": 0,
             "movie_link": 0,
             "name": 0,
             "role_type": 0,
             "title": 0,
             "movie_info": 0,
             "person_info": 0,
}
tablesWithSel = {}
sortedTables = []
def min_selectivity(input):
    state = []
    solution = {}
    joinedTables, parsed_query, alias = queryParser(input)
    tables = get_tableWithSelectivity(parsed_query)
    conn, cursor = connect_bdd("imdb")
    # calculate cardinality
    for t in my_tables:
        query = "select count(*) from " + t
        cursor.execute(query)
        my_tables[t] = cursor.fetchall()[0][0]
    # calculate selectivity
    for key in tables.keys():
        if key == '':
            continue
        json = {'select': {'value': {'count': '*'}}, 'from': {'value': alias[key], 'name': key}}
        if len(tables[key]) == 1:
            json['where'] = tables[key][0]
        else:
            json['where'] = {'and': tables[key]}
        query = moz_sql_parser.format(json)
        cursor.execute(query)
        tablesWithSel[alias[key]] = cursor.fetchall()[0][0]
    # add tables without selectivity
    if len(joinedTables) > len(tablesWithSel):
        for t in joinedTables:
            if t not in tablesWithSel:
                tablesWithSel[t] = my_tables[t]
    sortedTables = {k: v for k, v in sorted(tablesWithSel.items(), key=lambda item: item[1])}
    state = list(sortedTables.keys())
    my_parsed_query = moz_sql_parser.parse(input)
    new_from_clause = reorder_tables(my_parsed_query["from"], state)
    my_parsed_query["from"] = new_from_clause
    new_query = moz_sql_parser.format(my_parsed_query)
    cost = get_solution_cost(new_query)
    return new_query, cost


def reorder_tables(table_list, order_list):
    # Create a dictionary to map table names to their aliases
    table_dict = {table['name']: table['value'] for table in table_list}

    # Create a new list of tables in the desired order
    new_table_list = []
    for table_name in order_list:
        if table_name in table_dict:
            new_table_list.append({'value': table_dict[table_name], 'name': table_name})

    # Append any remaining tables that are not in the order list
    for table in table_list:
        if table['name'] not in order_list:
            new_table_list.append(table)

    return new_table_list
