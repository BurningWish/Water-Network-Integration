import networkx as nx
import step0_shp2nx
from shapely.geometry import Point, LineString
import pickle
import fiona
from shapely.wkt import loads
import sys


def find_nearest_nodes(i, g1, g2):
    op_n1 = (-1, -1)
    op_n2 = (-1, -1)
    dist = 999999999999999
    count = 0
    for n2 in g2:
        count += 1
        total_count = g2.number_of_nodes()
        print(i, count, total_count)
        for n1 in g1:
            p1 = Point(n1)
            p2 = Point(n2)
            current_dist = p1.distance(p2)
            if current_dist < dist:
                dist = current_dist
                op_n1 = n1
                op_n2 = n2
    
    return [op_n1, op_n2]

raw_graph = shp2nx.read_shp("step0_input//Edges.shp")

sys.exit()

graphs = list(nx.connected_component_subgraphs(raw_graph))

# find the largest graph
largest_id = -1
largest_size = -1
for i, g in enumerate(graphs):
    current_size = g.number_of_edges()
    if current_size > largest_size:
        largest_id = i
        largest_size = current_size
        
# Get the nodal pairs that we need to add
nodal_pairs = []
g1 = graphs[largest_id]

for i, g in enumerate(graphs):
    if i!= largest_id:
        g2 = g
        nodal_pair = find_nearest_nodes(i, g1, g2)
        nodal_pairs.append(nodal_pair)

"""   
# save the stupid nodal pairs
f = open("nodal_pairs", "wb")
pickle.dump(nodal_pairs, f)
f.close()
"""

# load the stupid nodal pairs
ff = open("nodal_pairs", "rb")
nodal_pairs = pickle.load(ff)
ff.close()

# add these nodes back to raw_graph
for nodal_pair in nodal_pairs:
    n1 = nodal_pair[0]
    n2 = nodal_pair[1]
    wkt = LineString(nodal_pair).wkt
    raw_graph.add_edge(n1, n2)
    raw_graph.edge[n1][n2]['wkt'] = wkt
    
# Let's do one more thing add nodeid and edgeid to the raw_graph
nodeId = 0
for node in raw_graph.nodes():
    nodeId += 1
    raw_graph.node[node]['nodeId'] = 'oldn' + '_' + str(nodeId).zfill(10)
    
edgeId = 0
for edge in raw_graph.edges():
    edgeId += 1
    n1 = edge[0]
    n2 = edge[1]
    raw_graph[n1][n2]['edgeId'] = 'olde' + '_' + str(edgeId).zfill(10)
    
        
# let's output the result
sourceDriver = 'ESRI Shapefile'
sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
             'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
             'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

# first the edges
sourceSchema = {'properties': {'type': 'str',
                               'edgeid': 'str'}, 
                'geometry': 'LineString'}

fileName = "step0_output//Edges.shp"

with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:

    for edge in raw_graph.edges():
        n1 = edge[0]
        n2 = edge[1]
        record = {}
        wkt = raw_graph.edge[n1][n2]['wkt']
        edgeid = raw_graph.edge[n1][n2]['edgeId']
        coords = list(loads(wkt).coords)
        record['geometry'] = {'coordinates': coords, 'type': 'LineString'}  # NOQA
        record['properties'] = {'type': 'main',
                                'edgeid': edgeid}
        source.write(record)

# then the nodes
sourceSchema = {'properties': {'type': 'str',
                               'nodeid': 'str'},
                'geometry': 'Point'}

fileName = "step0_output//Nodes.shp"

with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:

    for n in raw_graph.nodes():
        nodeid = raw_graph.node[n]['nodeId']
        record = {}
        record['geometry'] = {'coordinates': n, 'type': 'Point'}  # NOQA
        record['properties'] = {'type': 'distribution',
                                'nodeid': nodeid}
        source.write(record)