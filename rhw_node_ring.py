from pickle_hash import hash_code_hex

class RhwNodeRing(object):
    def __init__(self, nodes):
        self._nodes = nodes

    #Return the node to which the given key hashes to based on the key+node_port weight
    def get_node(self, key):
        assert len(self._nodes) > 0
        weights = []
        for node in self._nodes:
            w = self.weight(node["port"], key)
            weights.append((w, node))
        _, node = max(weights) #node with highest weight
        return node

    #Return the weight for the key+node port.
    def weight(self, node, key):
        # a and b for calculating weight
        a = 1103515245
        b = 12345

        # using existing hash function for hasing node and key both
        hash_val = int(hash_code_hex(key.encode()), base=16)

        return (a * ((a * node + b) ^ hash_val) + b) % (2 ^ 31)