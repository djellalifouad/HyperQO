import numpy as np

from sql2fea import ALL_TYPES, LEAF_TYPES, JOIN_TYPES, TreeBuilderError
from ImportantConfig import Config
config = Config()
class ValueExtractor:
    def __init__(self, offset=config.offset, max_value=20):
        self.offset = offset
        self.max_value = max_value

    # def encode(self,v):
    #     return np.log(self.offset+v)/np.log(2)/self.max_value
    # def decode(self,v):
    #     # v=-(v*v<0)
    #     return np.exp(v*self.max_value*np.log(2))#-self.offset
    def encode(self, v):
        return int(np.log(2 + v) / np.log(config.max_time_out) * 200) / 200.
        return int(np.log(self.offset + v) / np.log(config.max_time_out) * 200) / 200.

    def decode(self, v):
        # v=-(v*v<0)
        # return np.exp(v/2*np.log(config.max_time_out))#-self.offset
        return np.exp(v * np.log(config.max_time_out))  # -self.offset

    def cost_encode(self, v, min_cost, max_cost):
        return (v - min_cost) / (max_cost - min_cost)

    def cost_decode(self, v, min_cost, max_cost):
        return (max_cost - min_cost) * v + min_cost

    def latency_encode(self, v, min_latency, max_latency):
        return (v - min_latency) / (max_latency - min_latency)

    def latency_decode(self, v, min_latency, max_latency):
        return (max_latency - min_latency) * v + min_latency

    def rows_encode(self, v, min_cost, max_cost):
        return (v - min_cost) / (max_cost - min_cost)

    def rows_decode(self, v, min_cost, max_cost):
        return (max_cost - min_cost) * v + min_cost


value_extractor = ValueExtractor()


def get_plan_stats(data):
    return [value_extractor.encode(data["Total Cost"]), value_extractor.encode(data["Plan Rows"])]


class TreeBuilderError2(Exception):
    def __init__(self, msg):
        self.__msg = msg


def is_join(node):
    return node["Node Type"] in JOIN_TYPES


def is_scan(node):
    return node["Node Type"] in LEAF_TYPES





class TreeBuilder2:
    def __init__(self):
        self.__stats = get_plan_stats
        self.id2aliasname = config.id2aliasname
        self.aliasname2id = config.aliasname2id

    def __relation_name(self, node):
        if "Relation Name" in node:
            return node["Relation Name"]

        if node["Node Type"] == "Bitmap Index Scan":
            # find the first (longest) relation name that appears in the index name
            name_key = "Index Name" if "Index Name" in node else "Relation Name"
            if name_key not in node:
                print(node)
            for rel in self.__relations:
                if rel in node[name_key]:
                    return rel


    def __alias_name(self, node):
        if "Alias" in node:
            return np.asarray([self.aliasname2id[node["Alias"]]])

        if node["Node Type"] == "Bitmap Index Scan":
            # find the first (longest) relation name that appears in the index name
            name_key = "Index Cond"  # if "Index Cond" in node else "Relation Name"
            if name_key not in node:
                print(node)
                raise TreeBuilderError("Bitmap operator did not have an index name or a relation name")
            for rel in self.aliasname2id:
                if rel + '.' in node[name_key]:
                    return np.asarray([-1])
                    return np.asarray([self.aliasname2id[rel]])

        #     raise TreeBuilderError("Could not find relation name for bitmap index scan")
        print(node)
        raise TreeBuilderError("Cannot extract Alias type from node")

    def __featurize_join(self, node):
        assert is_join(node)
        print("node in is join",node)
        # return [node["Node Type"],self.__stats(node),0,0]
        arr = np.zeros(len(ALL_TYPES))
        arr[ALL_TYPES.index(node["Node Type"])] = 1
        feature = np.concatenate((arr, self.__stats(node)))
        return feature

    def __featurize_scan(self, node):
        assert is_scan(node)
        # return [node["Node Type"],self.__stats(node),self.__alias_name(node)]
        arr = np.zeros(len(ALL_TYPES))
        arr[ALL_TYPES.index(node["Node Type"])] = 1
        feature = np.concatenate((arr, self.__stats(node)))
        return (feature,)

    def plan_to_feature_tree(self, plan):

        # children = plan["Plans"] if "Plans" in plan else []
        if "Plan" in plan:
            plan = plan["Plan"]
        children = plan["Plan"] if "Plan" in plan else (plan["Plans"] if "Plans" in plan else [])
        if len(children) == 1:
            child_value = self.plan_to_feature_tree(children[0])
            if "Alias" in plan and plan["Node Type"] == 'Bitmap Heap Scan':
                alias_idx_np = np.asarray([self.aliasname2id[plan["Alias"]]])
                if isinstance(child_value[1], tuple):
                    raise TreeBuilderError("Node wasn't transparent, a join, or a scan: " + str(plan))
                return (child_value[0],)
            return child_value
        # print(plan)
        if is_join(plan):
            assert len(children) == 2
            my_vec = self.__featurize_join(plan)
            left = self.plan_to_feature_tree(children[0])
            right = self.plan_to_feature_tree(children[1])
            # print('is_join',my_vec)
            return (my_vec, left, right)
       # if is_scan(plan):
       #     assert not children
       #     # print(plan)
       #     s = self.__featurize_scan(plan)
       #     # print('is_scan',s)
       #     return s
       # raise TreeBuilderError("Node wasn't transparent, a join, or a scan: " + str(plan))