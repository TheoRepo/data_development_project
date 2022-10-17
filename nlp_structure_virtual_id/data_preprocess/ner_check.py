# -*- coding:UTF-8 -*-
import sys


def find_all(sub, s):
    index_list = []
    index = s.find(sub)
    while index != -1:
        index_list.append(index)
        index = s.find(sub, index + 1)

    if len(index_list) > 0:
        return index_list
    else:
        return []

# txt格式为： 我爱中国	地点(实体标签名)	中国

file_r_1 = sys.argv[1]
file_w = sys.argv[2]

with open(file_r_1, 'r',encoding='utf-8') as f:
    data = f.readlines()

bio_list = []
mark = "#NER MSG#"
for d in data:
    d_msg, entity_name, entities = d.strip("\n").split("\t")
    label_list = ["O"] * len(d_msg)
    entity_list = entities.split("#ALGO_ITEM_SEP#")
    entity_list.sort(key=len, reverse=True)
    entities = "$$".join(entity_list)
    entities_label = []
    d_msg_test = d_msg
    for entity in entity_list:
        start_list = find_all(entity, d_msg_test)
        entity_len = len(entity)
        if len(start_list) == 1:
            idx = start_list[0]
            if set(label_list[idx:idx+entity_len]) == {'O'}:
                label_list[idx] = "B-"+entity_name
                label_list[idx+1:idx+entity_len] = ["I-"+entity_name]*(entity_len-1)
                entities_label.extend(["O"] * (entity_len+2))
                d_msg_test = d_msg_test[:idx]+' '*entity_len+d_msg_test[idx+entity_len:]
        # 文本中出现多个相同的实体内容、文本中没有该实体内容
        else:
            entities_label.extend(["B-"+entity_name]+["I-"+entity_name]*(entity_len-1)+["O"]*2)

    entities_label = entities_label[:-2]
    d_msg = entities + mark + d_msg
    label_list = entities_label + ["O"] * len(mark) + label_list
    bio_list.append("".join([i + "\t" + j + "\n" for i, j in zip(d_msg, label_list)]))

with open(file_w, "w", encoding="utf-8") as f:
    for d in bio_list:
        f.write(d+"\n")