import time
from random import random
import moz_sql_parser
from algos.helper_functions import get_solution_cost, get_join_order_cost, get_modified_query, neighborhood
import math


class Node:
    def __init__(self, state):
        self.state = state
        self.visits = 0
        self.reward = 0
        self.children = []
        self.parent = None  # Add a parent attribute to store reference to the parent node


def mcts(query, num_iterations):



    # Parse the query and extract the table names
    parsed_query = moz_sql_parser.parse(query)
    tables = parsed_query['from']

    # Initialize the root node with the current join order
    root = Node(tables)

    # print("rooooot state", root.state)
    print("-----------------------")

    def expand(node):
        # Generate all possible adjacent join orders from the current state
        neighbors = neighborhood(node.state)
        # print("neighbors lenght in expansion ", len(neighbors))
        for neighbor in neighbors:
            child = Node(neighbor)
            node.children.append(child)
            child.parent = node  # Set the parent of the child node to the current node
        # print("node children after expansion: ", node.children)

    def select(node):
        best_child = None
        best_ucb = float('-inf')
        for child in node.children:
            if child.visits == 0:  # Check if child has not been visited yet
                ucb = float('inf')  # Assign a large value for unvisited nodes to encourage exploration
            else:
                ucb = child.reward / child.visits + 2 * (math.log(node.visits) / (child.visits + 1e-8)) ** 0.5
            if ucb > best_ucb:
                best_ucb = ucb
                best_child = child
        # print("best child after selection: ", best_child.state)
        return best_child


    def simulate(node):
        import random
        current_node = node
        neighbors = neighborhood(current_node.state)
        random_neighbor = random.choice(neighbors)
        current_node = Node(random_neighbor)
        # print("current node state after simulation: ", current_node.state)
        return 1/get_join_order_cost(parsed_query, current_node.state)

    def backpropagate(node, reward):
        while node is not None:
            node.visits += 1
            node.reward += reward
            node = node.parent
    for _ in range(num_iterations):
        node = root

        # Select phase
        while node.children:
            node = select(node)
            # print("after selection phase", node)
            # print("-----------------------")


        # Expand phase
        if not node.visits:
            expand(node)
        # print("after expansion phase", node)
        # print("-----------------------")


        # Simulate phase
        reward = simulate(node)
        # print("after reward phase", reward)
        print("-----------------------")


        # Backpropagate phase
        # print("starting propgation phase")
        backpropagate(node, reward)
        # print("done with propgation phase")

        print(node.state)


    # Choose the best join order from the root node
    best_child = max(root.children, key=lambda child: child.visits)
    # print("final best child: ", best_child.state)

    solution_query = get_modified_query(parsed_query, best_child.state)
    # print(solution_query)
    solution_cost = get_solution_cost(solution_query)


    return solution_query, solution_cost




