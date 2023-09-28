import json

dbt_sources = {}
dbt_models = {}
child_map = {}
lineage_map = {}

def get_node_name(node_name):
    if node_name.startswith("source"):
        return dbt_sources[node_name]["name"]  
    if node_name.startswith("model"):
        return dbt_models[node_name]["name"]

with open("manifest.json") as f:
     data = json.load(f)

dbt_sources = data["sources"]
dbt_models = data["nodes"]
child_map = data["child_map"]

for item in child_map:
    parent_name = get_node_name(item)
    child_list = []
    for i in range(len(child_map[item])):
        child_name = get_node_name(child_map[item][i])
        child_list.append(child_name)
   
    if len(child_list) > 0:
        lineage_map[parent_name] = child_list

dbt_lineage_map["lineage_map"] = lineage_map

with open('dbt_lineage_map.json', 'w') as f:
    content = json.dumps(dbt_lineage_map)
    f.write(content)
