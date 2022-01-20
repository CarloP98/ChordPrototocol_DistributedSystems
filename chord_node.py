import sys
from datetime import datetime
import threading
import time
import socket
import pickle
import hashlib
import collections

M = 7  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2 ** M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n


class ModRange(object):
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2 ** (k - 1)) % NODES
        self.next_start = (n + 2 ** k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.next_start, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


class ChordNode(object):
    def __init__(self):
        # start listening
        self.my_port = None
        self.listener_thread = threading.Thread(target=self.listen, args=())
        self.listener_thread.daemon = True
        self.listener_thread.start()
        while not self.my_port:
            pass

        self.predecessor = None
        self.node_id = self.get_node_id(self.my_port)
        time_now = datetime.now().strftime("[%H:%M:%S.%f]")
        print("{} node with id {} created.".format(time_now, self.node_id))
        print("{} node {} started listening on port {}".format(time_now, self.node_id, self.my_port))
        self.finger = [None] + [FingerEntry(self.node_id, k) for k in range(1, M + 1)]
        self.keys = {}

    @staticmethod
    def get_node_id(port_num):
        """Return node id given port number"""
        str_val = '127.0.0.1' + str(port_num)
        sha = hashlib.sha1(str_val.encode()).digest()
        sha_int = int.from_bytes(sha, byteorder='big')
        # use m rightmost bits as id
        return sha_int & ((1 << M) - 1)

    # creates listening port and starts listening
    def listen(self):
        """Start listening on an available port for RPC calls"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))
        sock.listen(BACKLOG)
        self.my_port = sock.getsockname()[1]

        while True:
            conn, addr = sock.accept()
            func, arg_1, arg_2 = pickle.loads(conn.recv(BUF_SZ))
            time_now = datetime.now().strftime("[%H:%M:%S.%f]")
            print("{} {}(arg1: {}, arg2: {})".format(time_now, func, self.truncate_string(str(arg_1)), self.truncate_string(str(arg_2))))
            request_handler = threading.Thread(target=self.process_request, args=(conn, func, arg_1, arg_2))
            request_handler.daemon = True
            request_handler.start()

    def print_status(self):
        """Log the node's finger table, predecessor, successor, and num of keys stored"""
        print("\n_________node {} status_________".format(self.node_id))
        print("predecessor: {}".format(self.get_node_id(self.predecessor)))
        print("successor: {}".format(self.get_node_id(self.successor)))
        # print('keys: {}'.format(['{0} '.format(k) for k, v in self.keys.items()]))
        print('num of keys: {}'.format(len(self.keys)))
        print("FINGER TABLE:".format(self.node_id))
        print(" {:<10} {:<10} {:<10}".format('id', 'succ', 'port'))
        for i in range(1, M + 1):
            print(" {:<10} {:<10} {:<10}".format(i, self.get_node_id(self.finger[i].node), self.finger[i].node))
        print("\n")

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, np):
        self.finger[1].node = np

    def find_successor(self, id):
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')

    def find_predecessor(self, id):
        np = self.my_port
        while id not in ModRange(self.get_node_id(np) + 1, self.get_node_id(self.call_rpc(np, 'successor')) + 1, NODES):
            np = self.call_rpc(np, 'closest_preceding_finger', id)
        return np

    def closest_preceding_finger(self, id):
        for i in range(M, 0, -1):
            if self.get_node_id(self.finger[i].node) in ModRange(self.node_id + 1, id, NODES):
                return self.finger[i].node
        return self.my_port

    def join_network(self, known_node_port):
        """try to join/start a Chord network"""
        time_now = datetime.now().strftime("[%H:%M:%S.%f]")
        if known_node_port != 0:
            print("{} trying to joining the network from node listening on port {}".format(time_now, known_node_port))
            # populate finger table
            self.init_finger_table(known_node_port)
            # ask successor for keys that belong to me
            self.call_rpc(self.successor, 'migrate_data')
            # update Chord finger tables
            self.update_others()
        else:
            print("{} Adding first node with id {} to the network".format(time_now, self.node_id))
            for i in range(1, M + 1):
                self.finger[i].node = self.my_port
            self.predecessor = self.my_port
        self.print_status()

    def init_finger_table(self, np):
        try:
            self.finger[1].node = self.call_rpc(np, 'find_successor', self.finger[1].start)
            if self.finger[1].node is None:
                exit(1)

            self.predecessor = self.call_rpc(self.successor, 'predecessor')
            self.call_rpc(self.successor, 'predecessor', self.my_port)
            for i in range(1, M):
                if self.finger[i + 1].start in ModRange(self.get_node_id(np), self.get_node_id(self.finger[i].node), NODES):
                    self.finger[i + 1].node = self.finger[i].node
                else:
                    self.finger[i + 1].node = self.call_rpc(np, 'find_successor', self.finger[i + 1].start)
        except Exception as e:
            print("Error while joining network from port {}".format(np))
            print(e)

    def update_others(self):
        for i in range(1, M + 1):
            p = self.find_predecessor((1 + self.node_id - 2 ** (i - 1) + NODES) % NODES)
            self.call_rpc(p, 'update_finger_table', self.my_port, i)

    def update_finger_table(self, s, i):
        s_id = self.get_node_id(s)
        if self.finger[i].start != self.get_node_id(self.finger[i].node) and s_id in ModRange(self.finger[i].start, self.get_node_id(self.finger[i].node), NODES):
            self.finger[i].node = s
            p = self.predecessor
            self.print_status()
            self.call_rpc(p, 'update_finger_table', s, i)
        else:
            self.print_status()

    def call_rpc(self, np, func, arg_1=None, arg_2=None):
        """Start a thread to handle RPC call when listener receives an RPC call"""
        address = ('localhost', np)
        node_id = self.get_node_id(np)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            time_now = datetime.now().strftime("[%H:%M:%S.%f]")
            sock.connect(address)
            print("{} node{} --> node{}: {}(arg1: {}, arg2: {})".format(time_now, self.node_id, node_id, func, self.truncate_string(str(arg_1)), self.truncate_string(str(arg_2))))
            sock.sendall(pickle.dumps((func, arg_1, arg_2)))
            return pickle.loads(sock.recv(BUF_SZ))
        except Exception as e:
            sock.close()
            print("Error: {}".format(e))
            return None
        sock.close()

    def add_key(self, key, data):
        """add/update key in node's dictionary"""
        self.keys[key] = data
        return "Added"

    def get_key_data(self, key):
        """return key's data"""
        if key in self.keys:
            return self.node_id, self.keys[key]
        else:
            return self.node_id, "Not found"

    def populate(self, key, value):
        """find successor of key and add key to its dictionary"""
        # encode key
        sha = hashlib.sha1(key.encode()).digest()
        sha_int = int.from_bytes(sha, byteorder='big')
        # use m rightmost bits as id
        encoded_key = sha_int & ((1 << M) - 1)
        time_now = datetime.now().strftime("[%H:%M:%S.%f]")
        print("{} received new data with key '{}' from client, finding successor...".format(time_now, encoded_key))
        successor = self.find_successor(encoded_key)
        return self.call_rpc(successor, 'add_key', key, value)

    def query(self, key):
        """find successor of queried key and ask for that key's data"""
        sha = hashlib.sha1(key.encode()).digest()
        sha_int = int.from_bytes(sha, byteorder='big')
        # use m rightmost bits as id
        encoded_key = sha_int & ((1 << M) - 1)

        successor = self.find_successor(encoded_key)
        return self.call_rpc(successor, 'get_key_data', key)

    def migrate_data(self):
        """FOR EXTRA CREDIT: send data that belongs to new predecessor just added"""
        for key, value in dict(self.keys).items():
            sha = hashlib.sha1(key.encode()).digest()
            sha_int = int.from_bytes(sha, byteorder='big')
            encoded_key = sha_int & ((1 << M) - 1)
            if encoded_key < self.get_node_id(self.predecessor):
                self.call_rpc(self.predecessor, 'add_key', key, value)
                del self.keys[key]

    def process_request(self, conn, func, arg_1, arg_2):
        if hasattr(self, func):
            if func == 'successor':
                conn.sendall(pickle.dumps(self.successor))
            elif func == 'predecessor':
                if arg_1 is not None:
                    self.predecessor = arg_1
                else:
                    conn.sendall(pickle.dumps(self.predecessor))
            elif func == 'query':
                response = self.query(arg_1)
                conn.sendall(pickle.dumps(response))
            elif func == 'populate':
                response = self.populate(arg_1, arg_2)
                conn.sendall(pickle.dumps(response))
            else:
                method = getattr(self, func)

                if arg_1 is not None and arg_2 is not None:
                    result = method(arg_1, arg_2)
                elif arg_1 is not None:
                    result = method(arg_1)
                else:
                    result = method()
                conn.sendall(pickle.dumps(result))
        else:
            print("Received an invalid RPC method")
        conn.close()

    @staticmethod
    def truncate_string(s):
        return s if len(s) < 30 else s[:30] + "..."


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python chord_node.py [known_node_port(use 0 to create a new chord)]".format(NODES - 1))
        exit(0)
    if not sys.argv[1].isdigit():
        print("invalid argument: {}, integer expected\nUsage: python chord_node.py [known_node_port(use 0 to create a new chord)]".format(sys.argv[1], NODES - 1))
        exit(0)

    # known port
    port = int(sys.argv[1])
    # create new node
    node = ChordNode()
    # join or start a network
    node.join_network(port)

    # using daemon threads, keeping main thread alive
    while True:
        time.sleep(1)
