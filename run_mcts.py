import random
import sys
import time

import serverParser.parserFinal
from energymodule import energy
from ImportantConfig import Config
from  serverParser import parserFinal

from PGUtils import pgrunner
config = Config()
from sql2fea import TreeBuilder, value_extractor
from NET import TreeNet
from sql2fea import Sql2Vec
from TreeLSTM import SPINN
sys.stdout = open(config.log_file, "w")
random.seed(113)
with open(config.queries_file) as f:
    import json
    queries = json.load(f)
tree_builder = TreeBuilder()
sql2vec = Sql2Vec()
value_network = SPINN(head_num=config.head_num, input_size=7 + 2, hidden_size=config.hidden_size, table_num=50,
                      sql_size=40 * 40 + config.max_column).to(config.device)
for name, param in value_network.named_parameters():
    from torch.nn import init

    if len(param.shape) == 2:
        init.xavier_normal(param)
    else:
        init.uniform(param)

net = TreeNet(tree_builder=tree_builder, value_network=value_network)
from Hinter import Hinter
from mcts import MCTSHinterSearch

mcts_searcher = MCTSHinterSearch()
hinter = Hinter(model=net, sql2vec=sql2vec, value_extractor=value_extractor, mcts_searcher=mcts_searcher)

print(len(queries))
s1 = 0
s2 = 0
s3 = 0
s4 = 0
s_pg = 0
s_hinter = 0
for epoch in range(1):
    for idx, x in enumerate(queries[:]):
        print('----', idx, '-----')
        pg_plan_time, pg_latency, mcts_time, hinter_plan_time, MPHE_time, hinter_latency, actual_plans, actual_time,plan_json_pg,plan_json_hinter,choosen_leading_pair,index_leading = hinter.hinterRun(
            x[0])
        print(x[0])
        pg_latency /= 1000
        hinter_latency /= 1000
        pg_plan_time /= 1000
        hinter_plan_time /= 1000
        print('pg plan:', pg_plan_time,'pg run:', pg_latency)
        s1 += pg_plan_time
        print('mcts:', mcts_time, 'plan gen:', hinter_plan_time, 'MPHE:', MPHE_time, 'hinter latency:', hinter_latency)
        s2 += mcts_time
        s3 += hinter_plan_time
        s4 += MPHE_time
        s_pg += pg_latency
        s_hinter += sum(actual_time) / 1000
        # print()
        print([actual_plans, actual_time])
        print("%.4f %.4f %.4f %.4f %.4f %.4f %.4f" % (s1, s2, s3, s4, s_pg, s_hinter, s_hinter / s_pg))
        print('plan json pg', plan_json_pg)
        power_pg, exec_time_pg, energy_pg = energy.get_query_exec_energy(x[0])
        print('plan json hinter',plan_json_hinter)
        power_hinter, exec_time_hinter, energy_hinter = energy.get_query_exec_energy(choosen_leading_pair+x[0])
        file1 = open('scaler.txt', 'r')
        Lines = file1.readlines()
        count = 0
        # Strips the newline character
        join_order_pg = ""
        join_order_hinter = ""
        for line in Lines:
            if count == 0:
                join_order_pg = line
            if count == index_leading:
                join_order_hinter = line
            count = count+1
            print("Line{}: {}".format(count, line.strip()))
        join_order_pg = join_order_pg[:-1]
        join_order_hinter = join_order_hinter[:-1]
        number_join = len(join_order_pg.split(','))-1
        print('pg',power_pg, exec_time_pg, energy_pg)
        print('hinter',power_hinter, exec_time_hinter, energy_hinter)
        if energy_pg == float('inf'):
            energy_pg = -1
            exec_time_pg = -1
        if energy_hinter == float('inf'):
            energy_hinter = -1
            exec_time_hinter = -1
        json_plan_pg_real = None
        json_plan_hinter_real = None
        if exec_time_pg != -1:
            json_plan_pg_real = pgrunner.getAnalysePlanJson(sql=x[0])
        if exec_time_hinter != -1:
            json_plan_hinter_real = pgrunner.getAnalysePlanJson(choosen_leading_pair+x[0])
        serverParser.parserFinal.create_query(query=x[0],
                                              prefix_algo='mcts',
                                              converge= s_hinter / s_pg,
                                              prefix_search_time=None,
                                              json_plan_hybride=plan_json_hinter,
                                              json_plan_pg=plan_json_pg,
                                              execution_energy_pg=energy_pg,
                                              execution_time_hybride=exec_time_hinter,
                                              results=[actual_plans[-1],choosen_leading_pair,join_order_pg,join_order_pg,number_join],
                                              execution_time_pg=exec_time_pg,
                                              execution_energy_hybrid=energy_hinter,
                                              json_plan_pg_real =json_plan_pg_real,
                                              json_plan_hinter_real=json_plan_hinter_real,
                                              prefix_search_energy=None)
        import json
        sys.stdout.flush()
# Start traversing from the root node
