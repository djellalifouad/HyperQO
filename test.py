import json


import json


def get_join_order(plan):
    join_order = []
    if "Plans" not in plan:
        return []
    join_type = plan["Node Type"]
    if join_type == 'Nested Loop':
        print(plan['Plans'])
    for subplan in plan["Plans"]:
        sub_join_order = get_join_order(subplan)
        join_order.extend(sub_join_order)
    join_conditions = []
    if "Hash Cond" in plan:
        join_conditions.append(plan["Hash Cond"])
    if "Relation Name" in plan:
        join_conditions.append(plan["Relation Name"])
    if join_type == "Nested Loop" or join_type == "Hash Join" or join_type == "Merge Join":
        join_order.append((join_type, join_conditions))
    return join_order

# load the JSON data
with open("data.json") as f:
    data = json.load(f)
    order = []
    print(get_join_order(data,))


# traverse the JSON data