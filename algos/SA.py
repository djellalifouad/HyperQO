import moz_sql_parser
import random
import math
from algos.helper_functions import connect_bdd, get_join_order_cost

from adapterToHybrid.HybridAdapter import HybridAdapter

def simulated_annealing(query, num_iterations, initial_temperature, cooling_rate):
    # Parse the query and extract the table names
    parsed_query = moz_sql_parser.parse(query)
    tables = parsed_query['from']
    best_join_order = tables.copy()
    best_cost = get_join_order_cost(parsed_query, best_join_order)

    # Set the initial join order and its cost
    current_join_order = tables.copy()
    current_cost = best_cost

    # Set the temperature
    temperature = initial_temperature


    for iteration in range(num_iterations):
        # Generate a random neighbor
        neighbor = get_random_neighbor(current_join_order)

        # Calculate the cost of the neighbor
        neighbor_cost = get_join_order_cost(parsed_query, neighbor)

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


        # Cool down the temperature
        temperature *= cooling_rate

    # Reconstruct the query with the best join order
    parsed_query['from'] = best_join_order
    optimal_query = moz_sql_parser.format(parsed_query)

    return optimal_query, best_cost
# Define the neighborhood function that generates random adjacent join orders
def get_random_neighbor(join_order):
    neighbor = join_order.copy()
    i, j = sorted(random.sample(range(len(join_order)), 2))
    neighbor[i:j+1] = reversed(neighbor[i:j+1])
    return neighbor
# Define the acceptance probability function
def get_acceptance_probability(current_cost, neighbor_cost, temperature):
    if neighbor_cost < current_cost:
        return 1.0
    else:
        return math.exp((current_cost - neighbor_cost) / temperature)
#with open('../JOB-queries/'+ str("2b") + '.sql', 'r') as file:
  #  query = file.read()
#print(HybridAdapter.adaptReturn(simulated_annealing(query,10, 100 ,0.5)[0]))


