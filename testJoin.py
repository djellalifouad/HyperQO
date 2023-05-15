from ImportantConfig import Config
config = Config()
import torch
def traverse_tree(node,joinOrder = []):
    if isinstance(node, tuple):
        for child in node:
            traverse_tree(child)
    else:
        if node[0].shape == torch.Size([]):
            file = open('scaler.txt', 'a')
            file.write(str(config.id2aliasname[node[0].item()])+ ",")
            file.close()
