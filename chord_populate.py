import csv
import sys
import hashlib
import chord_node
import socket
import pickle


def encode_key(key):
    sha = hashlib.sha1(key.encode()).digest()
    sha_int = int.from_bytes(sha, byteorder='big')
    # use m rightmost bits as id
    return sha_int & ((1 << chord_node.M) - 1)


def addEntity(port, key, value):
    address = ('localhost', port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(address)
        print("sending data with key '{}'({}) to known port {:5}...".format(key, encode_key(key), port), end="    ")
        sock.sendall(pickle.dumps(('populate', key, value)))
        if pickle.loads(sock.recv(chord_node.BUF_SZ)) == "Added":
            print("Added.")
    except Exception as e:
        print("Error: {}".format(e))
        exit(123)
    sock.close()


def populateChord(port, file, num_of_rows=None):
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader, None)  # skip the headers
        r = 0
        for row in csv_reader:
            r += 1
            player_id, year = row[0], row[3]
            key = player_id + year
            addEntity(port, key, row)
            if num_of_rows and r == num_of_rows:
                break


if len(sys.argv) != 3:
    print("Usage: python chord_populate.py [known_node_port] [file]")
    exit(0)
node_port = int(sys.argv[1])
file_path = sys.argv[2]

#populateChord(node_port, file_path, None)  # all rows
populateChord(node_port, file_path, num_of_rows=100)
