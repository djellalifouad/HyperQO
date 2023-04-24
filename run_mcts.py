import random
import sys
from ImportantConfig import Config
config = Config()
from sql2fea import TreeBuilder,value_extractor
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
value_network = SPINN(head_num=config.head_num, input_size=7+2, hidden_size=config.hidden_size, table_num = 50,sql_size = 40*40+config.max_column).to(config.device)
for name, param in value_network.named_parameters():
    from torch.nn import init
    if len(param.shape)==2:
        init.xavier_normal(param)
    else:
        init.uniform(param)
net = TreeNet(tree_builder= tree_builder,value_network = value_network)
from Hinter import Hinter
from mcts import MCTSHinterSearch
mcts_searcher = MCTSHinterSearch()
hinter = Hinter(model = net,sql2vec = sql2vec,value_extractor = value_extractor,mcts_searcher = mcts_searcher)
print(len(queries))
s1 = 0
s2 = 0
s3 =0
s4 = 0
s_pg = 0
s_hinter = 0
totalTime  = 0
print('7 tables')
print('résultat des 12 tables')
folder_path = './datasetS/12/'
import  os
index = 0
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
files_sorted = sorted(files)
print('hna')
print(len(files_sorted))
for filename in files_sorted:
    print('index',index)
    index += 1
    if index > 200:
        break
    # Get the full path of the file
    filepath = os.path.join(folder_path, filename)
    print('filepath',filepath)
    with open(filepath, 'r') as file:
        # Read the contents of the file
        file_contents = file.read()
        print('----', filename, '-----')
        realTime ,pg_plan_time, pg_latency, mcts_time, hinter_plan_time, MPHE_time, hinter_latency, actual_plans, actual_time = hinter.hinterRun(file_contents)
        print("real time:",realTime)
        pg_latency /= 1000
        hinter_latency /= 1000
        pg_plan_time /= 1000
        hinter_plan_time /= 1000
        print('pg plan:', pg_plan_time, 'pg run:', pg_latency)
        totalTime += realTime
        print("totalTime:", totalTime)
        s1 += pg_plan_time
        print('mcts:', mcts_time, 'plan gen:', hinter_plan_time, 'MPHE:', MPHE_time, 'hinter latency:', hinter_latency)
        s2 += mcts_time
        s3 += hinter_plan_time
        s4 += MPHE_time
        s_pg += pg_latency
        s_hinter += sum(actual_time) / 1000
        print("SQL :", file_contents)
        print([actual_plans, actual_time])
        print("%.4f %.4f %.4f %.4f %.4f %.4f %.4f" % (s1, s2, s3, s4, s_pg, s_hinter, s_hinter / s_pg))
    import json
s1 = 0
s2 = 0
s3 = 0
s4 = 0
s_pg = 0
s_hinter = 0
totalTime  = 0
print('7 tables')
folder_path = './datasetS/7/'
print('résultat des 7 tables')
index = 0
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
files_sorted = sorted(files)
print('hna')
print(len(files_sorted))
for filename in files_sorted:
    print('index',index)
    index += 1
    if index > 200:
        break
    # Get the full path of the file
    filepath = os.path.join(folder_path, filename)
    print('filepath',filepath)
    with open(filepath, 'r') as file:
        # Read the contents of the file
        file_contents = file.read()
        print('----', filename, '-----')
        realTime ,pg_plan_time, pg_latency, mcts_time, hinter_plan_time, MPHE_time, hinter_latency, actual_plans, actual_time = hinter.hinterRun(file_contents)
        print("real time:",realTime)
        pg_latency /= 1000
        hinter_latency /= 1000
        pg_plan_time /= 1000
        hinter_plan_time /= 1000
        print('pg plan:', pg_plan_time, 'pg run:', pg_latency)
        totalTime += realTime
        print("totalTime:", totalTime)
        s1 += pg_plan_time
        print('mcts:', mcts_time, 'plan gen:', hinter_plan_time, 'MPHE:', MPHE_time, 'hinter latency:', hinter_latency)
        s2 += mcts_time
        s4 += MPHE_time
        s_pg += pg_latency
        s_hinter += sum(actual_time) / 1000
        print("SQL :", file_contents)
        print([actual_plans, actual_time])
        print("%.4f %.4f %.4f %.4f %.4f %.4f %.4f" % (s1, s2, s3, s4, s_pg, s_hinter, s_hinter / s_pg))
    import json
s1 = 0
s2 = 0
s3 = 0
s4 = 0
s_pg = 0
s_hinter = 0
totalTime  = 0
print('7 tables')
folder_path = './datasetS/17/'
print('résultat des 17 tables')
index = 0
files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
files_sorted = sorted(files)
print('hna')
print(len(files_sorted))
for filename in files_sorted:
    print('index',index)
    index += 1
    if index > 200:
        break
    # Get the full path of the file
    filepath = os.path.join(folder_path, filename)
    print('filepath',filepath)
    with open(filepath, 'r') as file:
        # Read the contents of the file
        file_contents = file.read()
        print('----', filename, '-----')
        realTime ,pg_plan_time, pg_latency, mcts_time, hinter_plan_time, MPHE_time, hinter_latency, actual_plans, actual_time = hinter.hinterRun(file_contents)
        print("real time:",realTime)
        pg_latency /= 1000
        hinter_latency /= 1000
        pg_plan_time /= 1000
        hinter_plan_time /= 1000
        print('pg plan:', pg_plan_time, 'pg run:', pg_latency)
        totalTime += realTime
        print("totalTime:", totalTime)
        s1 += pg_plan_time
        print('mcts:', mcts_time, 'plan gen:', hinter_plan_time, 'MPHE:', MPHE_time, 'hinter latency:', hinter_latency)
        s2 += mcts_time
        s4 += MPHE_time
        s_pg += pg_latency
        s_hinter += sum(actual_time) / 1000
        print("SQL :", file_contents)
        print([actual_plans, actual_time])
        print("%.4f %.4f %.4f %.4f %.4f %.4f %.4f" % (s1, s2, s3, s4, s_pg, s_hinter, s_hinter / s_pg))
    import json
sys.stdout.flush()