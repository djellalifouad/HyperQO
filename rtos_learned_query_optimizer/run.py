# Copyright 2018-2021 Tsinghua DBGroup
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import os

from rtos_learned_query_optimizer.PGUtils import pgrunner
#from energy.energy_functions import *
from rtos_learned_query_optimizer.sqlSample import sqlInfo
import numpy as np
from itertools import count
from math import log
import random
import time
from rtos_learned_query_optimizer.DQN import DQN,ENV
from rtos_learned_query_optimizer.TreeLSTM import SPINN
from rtos_learned_query_optimizer.JOBParser import DB
import copy
import torch
from torch.nn import init
from rtos_learned_query_optimizer.ImportantConfig import Config

config = Config()

device = torch.device("cuda" if torch.cuda.is_available() and config.usegpu==1 else "cpu")


with open(config.schemaFile, "r") as f:
   createSchema = "".join(f.readlines())

db_info = DB(createSchema)

featureSize = 128

policy_net = SPINN(n_classes = 1, size = featureSize, n_words = 100,mask_size= 40*41,device=device).to(device)
target_net = SPINN(n_classes = 1, size = featureSize, n_words = 100,mask_size= 40*41,device=device).to(device)
policy_net.load_state_dict(torch.load("./rtos_learned_query_optimizer/saved_model/best_trained_20k_queries.pth"))
target_net.load_state_dict(policy_net.state_dict())
target_net.eval()

DQN = DQN(policy_net,target_net,db_info,pgrunner,device)

def getJoinOrder(query):
            print(query)
            sqlSample = sqlInfo(pgrunner,query,"input")
            # pg_cost = sql.getDPlantecy()
            env = ENV(sqlSample,db_info,pgrunner,device,run_mode = False)
            print("-----------------------------")
            for t in count():
                try:
                 action_list, chosen_action,all_action = DQN.select_action(env,need_random=False)
                 left = chosen_action[0]
                 right = chosen_action[1]
                 env.takeAction(left,right)
                 reward, done= env.reward_new()
                except Exception:
                    with open("./rtos_learned_query_optimizer/HintLatest.txt", "r") as text_file:
                        prefix = text_file.readline()
                        return prefix.split(" ")



