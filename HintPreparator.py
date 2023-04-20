import torch

from ImportantConfig import Config
from NET import ValueNet
from adapterToHybrid.HybridAdapter import HybridAdapter
from algos.II import iterative_improvement
from algos.MinSel import min_selectivity
from algos.SA import simulated_annealing
from sql2fea import Sql2Vec

config = Config()
model_path = './model/log_c3_h64_s4_t3.pth'
from NET import ValueNet
predictionNet = ValueNet(config.mcts_input_size).to(config.cpudevice)
for name, param in predictionNet.named_parameters():
    from torch.nn import init
    # print(name,param.shape)
    if len(param.shape)==2:
        init.xavier_normal(param)
    else:
        init.uniform(param)
predictionNet.load_state_dict(torch.load(model_path, map_location=lambda storage, loc: storage))
print(predictionNet.eval())
class HintPreparator:
        @staticmethod
        def getbestHint(query,queryEncode):
            #Hint List
            completJoinOrder =  []
            #call-each-algorithm
            completJoinOrder.append(HybridAdapter.adaptReturn(iterative_improvement(query, 4)[0]))
            completJoinOrder.append(HybridAdapter.adaptReturn(simulated_annealing(query,10, 100 ,0.5)[0]))
            completJoinOrder.append(HybridAdapter.adaptReturn(min_selectivity(query,)[0]))
            print(completJoinOrder)




###########################
#Hint preparator test :

with open(config.queries_file) as f:
    import json
    queries = json.load(f)
sql2vec = Sql2Vec()
HintPreparator.getbestHint(queries[0][0],sql2vec.to_vec(sql=queries[0][0])[0])
###########################
