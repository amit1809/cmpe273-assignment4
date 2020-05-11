import socket
import ast
from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, deserialize, serialize_DELETE
from ch_node_ring import ChNodeRing

BUFFER_SIZE = 1024

class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)       

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    ch_ring = ChNodeRing(NODES)
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        server = ch_ring.get_node(key)
        ch_server_id = NODES.index(ast.literal_eval(server)) #ast.literal to convert string output to dict element
        response = udp_clients[ch_server_id].send(data_bytes)
        hash_codes.add(response)
        print(response)

    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")

    # GET all users.
    for hc in hash_codes:
        data_bytes, key = serialize_GET(hc)
        key = key.decode("utf-8")
        server = ch_ring.get_node(key)
        ch_server_id = NODES.index(ast.literal_eval(server))
        response = udp_clients[ch_server_id].send(data_bytes)
        #print(deserialize(response))
        print(response)

    # DELETE all users
    for hc in hash_codes:
        data_bytes, key = serialize_DELETE(hc)
        key = key.decode("utf-8")
        server = ch_ring.get_node(key)
        ch_server_id = NODES.index(ast.literal_eval(server))
        response = udp_clients[ch_server_id].send(data_bytes)
        print(response)


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)
