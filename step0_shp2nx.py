import networkx as nx
import fiona
import shapely
from shapely.geometry import LineString, Point

def read_shp(edge_path):
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
            network.edge[startNode][endNode]['wkt']= new_line.wkt


    return network
