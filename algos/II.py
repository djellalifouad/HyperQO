from algos.helper_functions import connect_bdd, get_join_order_cost
import moz_sql_parser
import itertools

from adapterToHybrid.HybridAdapter import HybridAdapter

def iterative_improvement(query, num_permutations ):

    # Parse the query and extract the table names
    parsed_query = moz_sql_parser.parse(query)
    tables = parsed_query['from']
    # Initialize the best join order and its cost
    best_join_order = tables.copy()

    best_cost = get_join_order_cost(parsed_query, best_join_order)

    index = 0

    for starting_join_order in itertools.islice(itertools.permutations(tables), num_permutations):
        index += 1
        # Initialize the current join order and its cost
        current_join_order = list(starting_join_order)
        current_cost = get_join_order_cost(parsed_query, current_join_order)
        # Generate all possible adjacent join orders
        neighbors = neighborhood(current_join_order)
        improved = False
        # Evaluate the cost of each neighbor
        for neighbor in neighbors:
                neighbor_cost = get_join_order_cost(parsed_query, neighbor)
                # If the neighbor has a lower cost, select it as the new join order
                if neighbor_cost < current_cost:
                    current_join_order = neighbor.copy()
                    current_cost = neighbor_cost
                    # If the new join order is better than the best seen so far, update it
                    if current_cost < best_cost:
                        improved = True
                        best_join_order = current_join_order.copy()
                        best_cost = current_cost

                if(improved):
                    break
        # if(improved):
        #     break
    # Reconstruct the query with the best join order
    parsed_query['from'] = best_join_order

    optimal_query = moz_sql_parser.format(parsed_query)
    return optimal_query , best_cost
# Define the neighborhood function that generates adjacent join orders
def neighborhood(join_order):
        neighbors = []
        for i in range(len(join_order) - 1):
            for j in range(i + 1, len(join_order)):
                neighbor = join_order.copy()

                neighbor[i], neighbor[j] = neighbor[j], neighbor[i]
                neighbors.append(neighbor)
        return neighbors
#with open('../JOB-queries/'+ str("1a") + '.sql', 'r') as file:
   # query = file.read()

  #  print(HybridAdapter.adaptReturn(iterative_improvement(query,10)[0]))