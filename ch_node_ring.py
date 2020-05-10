from bisect import bisect
from pickle_hash import hash_code_hex


class ChNodeRing():

    def __init__(self, server_list, virtual_nodes=32):
        nodes = self.generate_nodes(server_list, virtual_nodes)
        hnodes = [self.hash(node) for node in nodes]
        hnodes.sort() #sorted hash ring of the nodes
        self.virtual_nodes = virtual_nodes
        self.nodes = nodes
        self.hnodes = hnodes
        self.nodes_map = {self.hash(node): node.split("-")[1] for node in nodes}
        #print(self.nodes_map)

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
