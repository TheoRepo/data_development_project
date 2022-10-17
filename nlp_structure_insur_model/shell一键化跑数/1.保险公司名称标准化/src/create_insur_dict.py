import json

file_name = '../data/保险公司名称.txt'
insur_name = '../config/dict_list_file.json'
insur_name_dict = {}

with open(file_name, 'r') as f:
    data = f.readlines()

clean_data = []
for i in data:
    clean_data.append(i.strip())

insur_name_dict['insur_name'] = clean_data

# with open(insur_name, 'w') as f:
#     f.write(json.dumps(insur_name_dict, ensure_ascii=False))