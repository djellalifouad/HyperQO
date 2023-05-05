from algos.SA import get_join_conds, get_random_neighbor
from algos.helper_functions import get_join_order_cost, neighborhood, get_modified_query
import moz_sql_parser
import itertools


def iterative_improvement(query, num_permutations,filename ):

    # Parse the query and extract the table names
    parsed_query = moz_sql_parser.parse(query)
    tables = parsed_query['from']

    # Initialize the best join order and its cost
    best_join_order = get_join_conds(parsed_query)
    try:
        best_cost = get_join_order_cost(query, best_join_order)
    except:
        best_cost = float('inf')

    # Set the initial join order and its cost
    # current_join_order = tables.copy()
    current_join_order = best_join_order
    current_cost = best_cost
    print("--------------init-------------", best_cost)
    # Loop over all possible starting join orders
    index = 0
    for iteration in range(num_permutations):
        index += 1
        # print("mother loop index", index)
        # Initialize the current join order and its cost
        # Generate possible adjacent join orders
        neighbor = get_random_neighbor(current_join_order)

        # Evaluate the cost of each neighbor
        try:
            neighbor_cost = get_join_order_cost(query, neighbor)
        except:
            neighbor_cost = float('inf')
            print(
                f"--------------------------------------------{filename}-----------------------------------------------")
            num_permutations += 1
            continue

            # If the neighbor has a lower cost, select it as the new join order
        if neighbor_cost < current_cost:
            current_join_order = neighbor.copy()
            current_cost = neighbor_cost
            print("current neighbor_cost: ", neighbor_cost)

            # If the new join order is better than the best seen so far, update it
            if current_cost < best_cost:
                best_join_order = current_join_order.copy()
                best_cost = current_cost
                print("best_cost: ", best_cost)


    # Reconstruct the query with the best join order
    # parsed_query['from'] = best_join_order
    # print("best join order: ", best_join_order)
    optimal_query, join_order = get_modified_query(query, best_join_order)
    print(
        f"--------------------------------------------{filename}--{best_cost}---------------------------------------------")
    return optimal_query, best_cost , join_order


