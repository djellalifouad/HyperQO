import moz_sql_parser
import random
import math
from algos.helper_functions import connect_bdd, get_join_order_cost, get_modified_query


def simulated_annealing(query, num_iterations, initial_temperature, cooling_rate):
    # Parse the query and extract the table names
    parsed_query = moz_sql_parser.parse(query)
    tables = parsed_query['from']
    # best_join_order = tables.copy()
    best_join_order  = get_join_conds(parsed_query)
    try:
       best_cost = get_join_order_cost(query, best_join_order)
    except:
        best_cost = float('inf')

    # Set the initial join order and its cost
    # current_join_order = tables.copy()
    current_join_order = best_join_order
    current_cost = best_cost
    print("--------------init-------------", best_cost)

    # Set the temperature
    temperature = initial_temperature

    for iteration in range(num_iterations):
        # print("mother loop index", iteration)
        # Generate a random neighbor
        # neighbor = get_random_neighbor(current_join_order)
        neighbor = get_random_neighbor(current_join_order)

        # Calculate the cost of the neighbor
        try:
            neighbor_cost = get_join_order_cost(query, neighbor)
            print("best cost", neighbor_cost)
        except:
           num_iterations = 1 + num_iterations
           continue

        # Calculate the acceptance probability
        acceptance_probability = get_acceptance_probability(current_cost, neighbor_cost, temperature)

        # Accept or reject the neighbor
        if acceptance_probability >= random.random():
            current_join_order = neighbor.copy()
            current_cost = neighbor_cost

            # If the new join order is better than the best seen so far, update it
            if current_cost < best_cost:
                best_join_order = current_join_order.copy()
                best_cost = current_cost
                print("best cost", best_cost)

        # Cool down the temperature
        temperature *= cooling_rate

    # Reconstruct the query with the best join order
    # parsed_query['from'] = best_join_order
    # optimal_query = moz_sql_parser.format(parsed_query)
    optimal_query, join_order = get_modified_query(query, best_join_order)

    return optimal_query, best_cost , join_order


# Define the neighborhood function that generates random adjacent join orders
def get_random_neighbor(join_order):
    neighbor = join_order.copy()
    i, j = sorted(random.sample(range(len(join_order)), 2))
    neighbor[i:j + 1] = reversed(neighbor[i:j + 1])
    return neighbor


# Define the acceptance probability function
def get_acceptance_probability(current_cost, neighbor_cost, temperature):
    if neighbor_cost < current_cost:
        return 1.0
    else:
        return math.exp((current_cost - neighbor_cost) / temperature)


def get_join_conds(parsed_query):

    table_aliases = [table['name'] for table in parsed_query['from']]

    join_conditions = parsed_query['where']['and']

    joins = []
    join_aliases = []
    for condition in join_conditions:

        if 'eq' in condition and isinstance(condition['eq'][0], str) and isinstance(condition['eq'][1], str)  and '.' in condition['eq'][0] and '.' in condition['eq'][1]:

            left = condition['eq'][0].split('.')[0]
            right = condition['eq'][1].split('.')[0]
            if left in table_aliases and right in table_aliases:
                joins.append(condition)
                join_aliases.append(left)
                join_aliases.append(right)

    return joins


import random

def shuffle_order(arr):
    new_arr = random.sample(arr, len(arr))
    return new_arr