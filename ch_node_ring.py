from bisect import bisect
from pickle_hash import hash_code_hex


class ChNodeRing():

    def __init__(self, server_list, virtual_nodes=32):

        #generate list of nodes and virtual nodes
        nodes = self.generate_nodes(server_list, virtual_nodes)

        #create hash node list and then sort it
        hnodes = [self.hash(node) for node in nodes]
        hnodes.sort() #sorted hash nodes list (hash ring)

        self.virtual_nodes = virtual_nodes
        self.nodes = nodes
        self.hnodes = hnodes

        #keeping hash(node) and node as key:value
        self.nodes_map = {self.hash(node): node.split("-")[1] for node in nodes}

    #using existing hash function for hasing node and key both
    def hash(self, val):
        m = hash_code_hex(val.encode())
        return m

    #To generate virtual nodes
    def generate_nodes(self, server_list, virtual_nodes):
        nodes = []
        for i in range(virtual_nodes):
            for server in server_list:
               nodes.append('{0}-{1}'.format(i, server))
        #print(nodes)
        return nodes

    # returns the next right node in the hash ring where hash(key) matches
    def get_node(self, key):
        pos = bisect(self.hnodes, self.hash(key)) #Find position of key in sorted hash ring using bisect
        if pos == len(self.hnodes):
            return self.nodes_map[self.hnodes[0]]
        else:
            return self.nodes_map[self.hnodes[pos]]

    # returns main node and 2 next nodes for data replication
    def get_node_with_replication(self, key):
        pos = bisect(self.hnodes, self.hash(key)) #Find position of key in sorted hash ring using bisect
        if pos == len(self.hnodes):
            return self.nodes_map[self.hnodes[0]], self.nodes_map[self.hnodes[1]], self.nodes_map[self.hnodes[2]]
        else:
            main_node = self.nodes_map[self.hnodes[pos]]
            if pos+1 == len(self.hnodes):
                node_2 = self.nodes_map[self.hnodes[0]]
                node_3 = self.nodes_map[self.hnodes[1]]
            elif pos+2 == len(self.hnodes):
                node_2 = self.nodes_map[self.hnodes[pos+1]]
                node_3 = self.nodes_map[self.hnodes[0]]
            else:
                node_2 = self.nodes_map[self.hnodes[pos + 1]]
                node_3 = self.nodes_map[self.hnodes[pos + 2]]
            return main_node, node_2, node_3

