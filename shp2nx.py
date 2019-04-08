import networkx as nx
import fiona
from shapely.geometry import LineString


def read_shp(edge_path, node_path):
    network = nx.Graph()

    with fiona.open(edge_path, 'r') as c:
        for record in c:
            old_coords = record['geometry']['coordinates']
            new_coords = []
            for coord in old_coords:
                x = round(coord[0], 3)
                y = round(coord[1], 3)
                new_coords.append((x, y))
            new_line = LineString(new_coords)
            startNode = new_coords[0]
            endNode = new_coords[-1]
            network.add_edge(startNode, endNode)
            network.edge[startNode][endNode]['wkt'] = new_line.wkt

            network.edge[startNode][endNode]['edgeType'] = record['properties']['type']  # NOQA
            network.edge[startNode][endNode]['edgeId'] = record['properties']['edgeid']  # NOQA
            network.edge[startNode][endNode]['nodeFrom'] = record['properties']['nodefrom']  # NOQA
            network.edge[startNode][endNode]['nodeTo'] = record['properties']['nodeto']  # NOQA
            network.edge[startNode][endNode]['source'] = record['properties']['source']  # NOQA

            network.edge[startNode][endNode]['Length'] = new_line.length  # NOQA
            network.edge[startNode][endNode]['Coords'] = new_coords

    with fiona.open(node_path, 'r') as c:
        for record in c:
            point = record['geometry']['coordinates']
            node = (round(point[0], 3), round(point[1], 3))
            network.node[node]['nodeType'] = record['properties']['type']
            network.node[node]['nodeId'] = record['properties']['nodeid']
            network.node[node]['source'] = record['properties']['source']
            network.node[node]['Coords'] = [node[0], node[1]]

    return network
