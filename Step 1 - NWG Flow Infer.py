import networkx as nx
import step1_shp2nx
import fiona
from shapely.wkt import loads

edge_path = "step0_output//Correct_Edges.shp"
node_path = "step0_output//Correct_Nodes.shp"

G = step1_shp2nx.read_shp(edge_path, node_path)

# find all the nodes who are sources at this moment
sources = [x for x, y in G.nodes(data=True) if y['type'] == 'source']

# loop thorough all the nodes to find which source is closer and assign to nodes
count = 0
for n in G.nodes():
    count += 1
    print(count)
    s1 = sources[0]
    s2 = sources[1]
    dist1 = nx.shortest_path_length(G, n, s1, weight = 'length')
    dist2 = nx.shortest_path_length(G, n, s2, weight = 'length')
    if dist1 <= dist2:
        G.node[n]['source'] = 1
    else:
        G.node[n]['source'] = 2
        
# Now assign source of edges
for edge in G.edges():
    s = edge[0]
    e = edge[1]
    if G.node[s]['source'] == G.node[e]['source']:
        G.edge[s][e]['source'] = G.node[s]['source']
    else:
        G.edge[s][e]['source'] = 3
    
# Now assign direction
count = 0
for edge in G.edges():
    count += 1
    print(count)
    n1 = edge[0]
    n2 = edge[1]
    if G.edge[n1][n2]['source'] == 3:
        G.edge[n1][n2]['nodeFrom'] = 'NULL'
        G.edge[n1][n2]['nodeTo'] = 'NULL'
    else:
        sid = G.edge[n1][n2]['source'] - 1
        source = sources[sid]
        dist1 = nx.shortest_path_length(G, n1, source, weight = 'length')
        dist2 = nx.shortest_path_length(G, n2, source, weight = 'length')
        if dist1 <= dist2:
            G.edge[n1][n2]['nodeFrom'] = G.node[n1]['nodeId']
            G.edge[n1][n2]['nodeTo'] = G.node[n2]['nodeId']
        else:
            G.edge[n1][n2]['nodeFrom'] = G.node[n2]['nodeId']
            G.edge[n1][n2]['nodeTo'] = G.node[n1]['nodeId']
            
# let's output the result
sourceDriver = 'ESRI Shapefile'
sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
             'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
             'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

# first the edges
sourceSchema = {'properties': {'type': 'str',
                               'edgeid': 'str',
                               'nodefrom': 'str',
                               'nodeto': 'str',
                               'source': 'int'}, 
                'geometry': 'LineString'}

fileName = "step1_output//Edges.shp"

with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:

    for edge in G.edges():
        n1 = edge[0]
        n2 = edge[1]
        record = {}
        wkt = G.edge[n1][n2]['wkt']
        edgeid = G.edge[n1][n2]['edgeId']
        nodefrom = G.edge[n1][n2]['nodeFrom']
        nodeto = G.edge[n1][n2]['nodeTo']
        edgetype = G.edge[n1][n2]['type']
        edgesource = G.edge[n1][n2]['source']
        coords = list(loads(wkt).coords)
        record['geometry'] = {'coordinates': coords, 'type': 'LineString'}  # NOQA
        record['properties'] = {'type': edgetype,
                                'edgeid': edgeid,
                                'nodefrom': nodefrom,
                                'nodeto': nodeto,
                                'source': edgesource}
        source.write(record)

# then the nodes
sourceSchema = {'properties': {'type': 'str',
                               'nodeid': 'str',
                               'source': 'int'},
                'geometry': 'Point'}

fileName = "step1_output//Nodes.shp"

with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:

    for n in G.nodes():
        nodeid = G.node[n]['nodeId']
        nodetype = G.node[n]['type']
        nodesource = G.node[n]['source']
        record = {}
        record['geometry'] = {'coordinates': n, 'type': 'Point'}  # NOQA
        record['properties'] = {'type': nodetype,
                                'nodeid': nodeid,
                                'source': nodesource}
        source.write(record)