from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T

def build_data_lineage(data_lineage_map):
    for node in data_lineage_map:
        if not g.V().hasLabel('lineage_node').has('node_name',node).hasNext():
            g.addV('lineage_node').property('node_name', node).next()
        for i in range(len(data_lineage_map[node])):
            child_node = data_lineage_map[node][i]
            if not g.V().hasLabel('lineage_node').has('node_name',child_node).hasNext():
                g.addV('lineage_node').property('node_name', child_node).next()
            g.V().has('node_name', node).addE('lineage_edge').property('edge_name',' ').to(__.V().has('node_name',child_node)).next()

connection = DriverRemoteConnection('wss://{neptune cluster endpoint}:8182/gremlin', 'g')
g = traversal().withRemote(connection)

with open("spline_lineage_map.json") as f:
     spline_lineage_map = json.load(f)

with open("dbt_lineage_map.json") as f:
     dbt_linage_map = json.load(f)

build_data_lineage(spline_lineage_map["lineage_map"])
build_data_lineage(dbt_lineage_map["lineage_map"])

remoteConn.close()