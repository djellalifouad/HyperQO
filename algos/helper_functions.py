import os
import random

import moz_sql_parser
import numpy as np
import psycopg2
from moz_sql_parser import parse

joinedTables = []
joinedClauses = []
alias = {}


def init():
    global joinedTables
    joinedTables = []
    global joinedClauses
    joinedClauses = []
    global alias
    alias = {}


def queryParser(input):
    init()

    parsed_query = parse(input)
    from_clause = parsed_query["from"]

    for j in range(0, len(from_clause)):
        joinedTables.append(from_clause[j]["value"])
        alias[from_clause[j]["name"]] = from_clause[j]["value"]

    where_clause = parsed_query["where"]["and"]
    k = 0
    while k < len(where_clause):
        if "eq" in where_clause[k]:
            if isinstance(where_clause[k]['eq'][1], str):
                joinedClauses.append(where_clause[k]['eq'])
                where_clause.remove(where_clause[k])
            else:
                k += 1
        else:
            k += 1

    return joinedTables, parsed_query, alias


def get_cost(query):
    conn, cursor = connect_bdd("stack")

    cursor.execute("explain (format json) " + query)
    file = cursor.fetchone()[0][0]
    result = file['Plan']["Total Cost"]

    disconnect_bdd(conn)
    return result


def get_query_latency(query, force_order):
    conn, cursor = connect_bdd("stack")
    # Prepare query
    join_collapse_limit = "SET join_collapse_limit ="
    join_collapse_limit += "1" if force_order else "8"
    query = join_collapse_limit + "; EXPLAIN  ANALYSE " + query + ";"

    cursor.execute(query)

    rows = cursor.fetchall()
    row = rows[0][0]
    latency = float(rows[0][0].split("actual time=")[1].split("..")[1].split(" ")[0])

    disconnect_bdd(conn)
    return latency


def get_solution_cost(query):
    conn, cursor = connect_bdd("stack")
    cursor.execute("SET join_collapse_limit =1;")

    cursor.execute("explain (format json) " + query)
    file = cursor.fetchone()[0][0]
    result = file['Plan']["Total Cost"]
    disconnect_bdd(conn)

    return result


def get_pg_cost(query):
    conn, cursor = connect_bdd("imdbload")

    cursor.execute("explain (format json) " + query)
    file = cursor.fetchone()[0][0]
    result = file['Plan']["Total Cost"]
    disconnect_bdd(conn)

    return result


def connect_bdd(name):
    conn = psycopg2.connect(host="localhost",
                            port=5401,
                            user="postgres", password="",
                            database=name)
    cursor = conn.cursor()
    return [conn, cursor]


def disconnect_bdd(conn):
    conn.close()


def get_tableWithSelectivity(parsed_query):
    result = {}
    where_clause = parsed_query["where"]["and"]
    for k in range(0, len(where_clause)):
        if "or" in where_clause[k]:
            print("ddddd", where_clause[k])
            table = list(list(where_clause[k].values())[0][0].values())[0][0].rpartition('.')[0]
            if (table not in result):
                result[table] = [where_clause[k]]
            else:
                result[table].append(where_clause[k])

        else:
            table = list(where_clause[k].values())[0][0].rpartition('.')[0]
            if (table not in result):
                result[table] = [where_clause[k]]
            else:
                result[table].append(where_clause[k])

    return result


def get_modified_query(query, join_conditions):
    from moz_sql_parser import parse, format

    new_from = ''

    parsed_query = parse(query)

    aliases_map = {}
    duplicate_table_diffrent_alias = {}
    table_names = [table['value'] for table in parsed_query['from']]
    table_aliases = [table['name'] for table in parsed_query['from']]
    for index, alias in enumerate(table_aliases):
        table = table_names[index]
        values_list = list(aliases_map.values())
        if table in values_list:
            duplicate_table_diffrent_alias[table] = alias
        aliases_map[alias] = table

    # print(" found dupes: ", duplicate_table_diffrent_alias)

    join_order = []
    helper_to_alter_where = []
    # create explicit join conditions
    i = 0
    for condition in join_conditions:
        # print(i)
        if 'eq' in condition and isinstance(condition['eq'][0], str) and isinstance(condition['eq'][1], str) and '.' in \
                condition['eq'][0] and '.' in condition['eq'][1]:
            i = i + 1
            # print(condition)

            left = condition['eq'][0].split('.')[0]
            right = condition['eq'][1].split('.')[0]
            if left in table_aliases and right in table_aliases and i == 1:

                new_from += f" FROM {aliases_map[left]} AS {left} JOIN {aliases_map[right]} AS {right} ON {condition['eq'][0]} = {condition['eq'][1]}"
                join_order.append(left)
                join_order.append(right)

            elif left in table_aliases and right in table_aliases and (i != 1):
                if right in join_order and left in join_order:
                    last_joined = join_order[-1]
                    # print("last_joined : ", last_joined)
                    if last_joined in duplicate_table_diffrent_alias.values():

                        if last_joined == right and right != duplicate_table_diffrent_alias[
                            aliases_map[right]]:
                            new_from += f" And  {condition['eq'][0]} = {condition['eq'][1]}"
                        elif last_joined == left and left != duplicate_table_diffrent_alias[
                            aliases_map[left]]:
                            new_from += f" And  {condition['eq'][0]} = {condition['eq'][1]}"

                        elif last_joined == right and right == duplicate_table_diffrent_alias[
                            aliases_map[right]]:
                            new_from += f" JOIN {aliases_map[right]} AS {right} ON {condition['eq'][0]} = {condition['eq'][1]}"
                            join_order.append(right)

                        elif last_joined == left and left == duplicate_table_diffrent_alias[
                            aliases_map[left]]:
                            new_from += f" JOIN {aliases_map[left]} AS {left} ON {condition['eq'][0]} = {condition['eq'][1]}"
                            join_order.append(left)

                    else:
                        if last_joined == right:
                            new_from += f" And  {condition['eq'][0]} = {condition['eq'][1]}"
                        elif last_joined == left:
                            new_from += f" And  {condition['eq'][0]} = {condition['eq'][1]}"


                elif right in join_order:
                    new_from += f" JOIN {aliases_map[left]} AS {left} ON {condition['eq'][0]} = {condition['eq'][1]}"
                    join_order.append(left)

                elif left in join_order:
                    new_from += f" JOIN {aliases_map[right]} AS {right} ON {condition['eq'][0]} = {condition['eq'][1]}"
                    join_order.append(right)

    for condition in parsed_query["where"]["and"]:
        if 'eq' in condition and isinstance(condition['eq'][0], str) and isinstance(condition['eq'][1], str)  and '.' in condition['eq'][0] and '.' in condition['eq'][1]:
            continue
        else:
            helper_to_alter_where.append(condition)
    # get only SELECT statement
    idx = query.index("FROM")
    select_stmt = query[:idx]
    # print("new selecet:", select_stmt)
    # print("new from:", new_from)

    # new_where
    parsed_query['where']['and'] = helper_to_alter_where
    new_query_with_updated_where = format(parsed_query)
    idx = new_query_with_updated_where.index("WHERE")
    where_stmt = new_query_with_updated_where[idx:]
    # print("new where_stmt:", where_stmt)

    return_query = f'{select_stmt} {new_from} {where_stmt}'
    return return_query, join_order


def get_join_order_cost(query, join_order):
    modified_query , join_order= get_modified_query(query, join_order)

    conn, cursor = connect_bdd("imdb")
    cursor.execute("SET join_collapse_limit = 1;")
    cursor.execute("explain (format json) " + modified_query)
    file = cursor.fetchone()[0][0]
    result = file['Plan']["Total Cost"]

    disconnect_bdd(conn)
    return result


def create_file(directory, filename, content):
    # Create the directory if it doesn't already exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create the file path by joining the directory and filename
    file_path = os.path.join(directory, filename)

    # Open the file in write mode and write the content to it
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"File {filename} created in directory {directory}")


# Define the neighborhood function that generates adjacent join orders
def neighborhood(join_order):
    neighbors = []
    for i in range(len(join_order) - 1):
        for j in range(i + 1, len(join_order)):
            neighbor = join_order.copy()
            neighbor[i], neighbor[j] = neighbor[j], neighbor[i]
            neighbors.append(neighbor)
    return neighbors
