import csv
import sys
import hashlib
import chord_node
import socket
import pickle


def query(port, search_key):
    address = ('localhost', port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(address)
        print("searching data with key '{}' from known port {:5}...".format(search_key, port))
        sock.sendall(pickle.dumps(('query', search_key, None)))
        node, data = pickle.loads(sock.recv(chord_node.BUF_SZ))
        print("response from successor(node {}): {}".format(node, data))
    except Exception as e:
        print("Error: {}".format(e))
        exit(6)
    sock.close()


if len(sys.argv) != 3:
    print("Usage: python chord_populate.py [known_node_port] [key](Player Id + Year)")
    exit(0)

node_port, item = int(sys.argv[1]), sys.argv[2]
query(node_port, item)




