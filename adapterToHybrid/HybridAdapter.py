import moz_sql_parser
import numpy as np

from ImportantConfig import Config

config = Config()
aliasname2id = {'kt1': 31, 'chn': 1, 'cn1': 29, 'mi_idx2': 36, 'cct1': 23, 'n': 21, 'a1': 39, 'kt2': 32,
                     'miidx': 18, 'it': 16, 'mi_idx1': 35, 'kt': 17, 'lt': 9, 'ci': 2, 't': 7, 'k': 8, 'start': 0,
                     'ml': 11, 'ct': 4, 't2': 28, 'rt': 6, 'it2': 13, 'an1': 37, 'at': 19, 'mc2': 34, 'pi': 26, 'mc': 5,
                     'mi_idx': 15, 'n1': 38, 'cn2': 30, 'mi': 14, 'it1': 12, 'cc': 22, 'cct2': 24, 'an': 20, 'mk': 10,
                     'cn': 3, 'it3': 25, 't1': 27, 'mc1': 33}
class HybridAdapter:
    @staticmethod
    def adaptReturn(query):
        print('return',query)
        parsed_query = moz_sql_parser.parse(query)
        tables = parsed_query['from']
        join_order =  np.zeros(config.max_hint_num)
        for index,alias in enumerate(tables):
            join_order[index]=aliasname2id[alias['name']]
        return join_order
    @staticmethod
    def adaptReturnRtos(tables):
        print("tables")
        print(tables)
        join_order =  np.zeros(config.max_hint_num)
        for index,alias in enumerate(tables):
            join_order[index]=aliasname2id[alias]
        return join_order
