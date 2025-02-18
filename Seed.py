#!/usr/bin/env python3
import socket
import pickle
import threading
import time
import ast  # for safe literal evaluation
import numpy as np
import datetime
import re

class Peer:
    """
    Represents an individual peer connection.
    Stores the IP, port, the associated socket, and a set of connections
    representing the peers that this node is directly connected to.
    """
    def __init__(self, ip, port, socket_obj):
        self.ip = ip
        self.port = port
        self.socket = socket_obj
        self.connections = set()  # Will hold tuples of (ip, port)

    def add_connection(self, peer_tuple):
        self.connections.add(peer_tuple)

    def __repr__(self):
        return f"Peer({self.ip}, {self.port}, connections={list(self.connections)})"

class SeedNodeConnections:
    """
    Manages the local node's connected peers.
    """
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.running = True
        self.neighbour = {}  # Mapping (ip, port) -> Peer instance
        self.count = 0

    def addNeighbour(self, ip, port, socket_obj):
        aggregate = (ip, port)
        if aggregate in self.neighbour:
            return False
        self.neighbour[aggregate] = Peer(ip, port, socket_obj)
        self.count += 1
        return True

    def removeNeighbour(self, ip, port):
        aggregate = (ip, port)
        if aggregate in self.neighbour:
            del self.neighbour[aggregate]
            self.count -= 1
            return True
        return False

class SeedNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.socket = None
        self.running = True
        # Regular (non-seed) peer connections.
        self.peer_connections = SeedNodeConnections(ip, port)
        # Seed node connections (mapping (ip, port) -> socket).
        self.seed_connections = {}
        # Known seed addresses (loaded from config.txt).
        self.known_seeds = []
        # Known non-seed peer addresses.
        self.known_peers = []
        # Network topology stored as: { peer_identity: set(other_peer_identities) }
        self.network_topology = {}
        # Log file name.
        self.log_file = f"seed_log_{self.port}.txt"
        self.log(f"Seed node started on {self.ip}:{self.port}")
        self.loadConfig()
        self.writeSelfToConfig()

    def log(self, message):
        """Logs a message with a timestamp to stdout and to the log file."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - {message}"
        print(log_message)
        try:
            with open(self.log_file, "a") as f:
                f.write(log_message + "\n")
        except Exception as e:
            print("Error writing log:", e)

    def loadConfig(self):
        try:
            with open("config.txt", "r") as file:
                lines = file.readlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    ip, port_str = line.split(":")
                    port = int(port_str)
                    if ip == self.ip and port == self.port:
                        continue
                    seed_tuple = (ip, port)
                    if seed_tuple not in self.known_seeds:
                        self.known_seeds.append(seed_tuple)
                except Exception as e:
                    self.log(f"Error parsing line in config.txt: {line} {e}")
        except Exception as e:
            self.log(f"Failed to open config.txt: {e}")

    def writeSelfToConfig(self):
        my_info = f"{self.ip}:{self.port}"
        try:
            try:
                with open("config.txt", "r") as file:
                    lines = file.read().splitlines()
            except FileNotFoundError:
                lines = []
            if my_info not in lines:
                with open("config.txt", "a") as file:
                    file.write(my_info + "\n")
                self.log(f"Wrote self info to config.txt: {my_info}")
            else:
                self.log(f"Self info already present in config.txt: {my_info}")
        except Exception as e:
            self.log(f"Error writing to config.txt: {e}")

    def get_peer_subset(self):
        keys = [k for k in self.peer_connections.neighbour.keys() if k != (self.ip, self.port)]
        return keys[:3] if len(keys) > 3 else keys

    def updatePeerConnections(self, new_peer, subset):
        subset_set = set(subset)
        if new_peer in self.network_topology:
            self.network_topology[new_peer] = self.network_topology[new_peer].union(subset_set)
        else:
            self.network_topology[new_peer] = subset_set

        for p in subset_set:
            if p in self.network_topology:
                self.network_topology[p].add(new_peer)
            else:
                self.network_topology[p] = {new_peer}

        if new_peer in self.peer_connections.neighbour:
            peer_instance = self.peer_connections.neighbour[new_peer]
            for p in subset_set:
                peer_instance.add_connection(p)
                if p in self.peer_connections.neighbour:
                    self.peer_connections.neighbour[p].add_connection(new_peer)

    def powerlaw_connect(self, new_peer, num_connections=4, alpha=2.0):
        available = [p for p in self.peer_connections.neighbour.keys() if p != new_peer]
        if len(available) < 1:
            self.log("Not enough peers available for power-law connection.")
            return

        sorted_peers = sorted(available, key=lambda p: len(self.peer_connections.neighbour[p].connections), reverse=True)
        weights = [(i + 1) - alpha for i in range(len(sorted_peers))]
        total = sum(weights)
        probs = [w / total for w in weights]
        selected = []
        while len(selected) < min(num_connections, len(sorted_peers)):
            idx = np.random.choice(len(sorted_peers), p=probs)
            candidate = sorted_peers[idx]
            if candidate not in selected:
                selected.append(candidate)
                probs[idx] = 0
                total = sum(probs)
                if total > 0:
                    probs = [p / total for p in probs]
        self.log(f"Power-law selected peers for {new_peer}: {selected}")
        if new_peer not in self.network_topology:
            self.network_topology[new_peer] = set()
        for peer in selected:
            self.network_topology[new_peer].add(peer)
            if peer in self.network_topology:
                self.network_topology[peer].add(new_peer)
            else:
                self.network_topology[peer] = {new_peer}
        if new_peer in self.peer_connections.neighbour:
            peer_instance = self.peer_connections.neighbour[new_peer]
            for peer in selected:
                peer_instance.add_connection(peer)
                if peer in self.peer_connections.neighbour:
                    self.peer_connections.neighbour[peer].add_connection(new_peer)

    def get_seeds_connected_to(self, new_peer):
        connected = []
        for seed, peer_obj in self.peer_connections.neighbour.items():
            if new_peer in peer_obj.connections:
                connected.append(seed)
        return connected

    def is_my_turn(self, new_peer, connected_seeds):
        candidates = connected_seeds.copy()
        self_tuple = (self.ip, self.port)
        if self_tuple not in candidates:
            candidates.append(self_tuple)
        candidates.sort(key=lambda x: (x[0], x[1]))
        index = hash(new_peer[0] + str(new_peer[1])) % len(candidates)
        return candidates[index] == self_tuple

    def broadcastNewNodeUpdate(self, new_peer, subset):
        msg = f"NewNodeUpdate|{new_peer}|{subset}\n"
        self.log("Broadcasting new node update: " + msg.strip())
        self.broadcastMessage(msg)

    def handleNewNodeUpdate(self, msg):
        try:
            parts = msg.split("|")
            if len(parts) != 3:
                self.log("Malformed NewNodeUpdate message: " + msg)
                return
            new_peer = ast.literal_eval(parts[1].strip())
            self.known_peers.append(new_peer)
            subset = ast.literal_eval(parts[2].strip())
            new_set = set(subset)
            if new_peer in self.network_topology:
                self.network_topology[new_peer] = self.network_topology[new_peer].union(new_set)
            else:
                self.network_topology[new_peer] = new_set
            for p in self.network_topology[new_peer]:
                if p in self.network_topology:
                    self.network_topology[p].add(new_peer)
                else:
                    self.network_topology[p] = {new_peer}
            if new_peer not in self.known_peers and new_peer not in self.known_seeds:
                self.known_peers.append(new_peer)
            self.log("Updated topology with new node: " + str(new_peer) +
                     " connections: " + str(self.network_topology[new_peer]))
        except Exception as e:
            self.log("Error handling NewNodeUpdate message: " + str(e))

    def setup(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))
        self.socket.listen()
        self.log(f"SeedNode is listening on {self.ip}:{self.port}")

    def acceptConnection(self):
        while self.running:
            try:
                clientSocket, clientAddress = self.socket.accept()
                f = clientSocket.makefile('r')
                handshake = f.readline().strip()
                if handshake.startswith("I am seed"):
                    try:
                        parts = handshake.split("|")
                        if len(parts) != 2:
                            raise Exception("Malformed handshake")
                        remote_seed = ast.literal_eval(parts[1].strip())
                    except Exception as e:
                        self.log("Error parsing seed handshake: " + str(e))
                        clientSocket.close()
                        continue
                    if remote_seed not in self.seed_connections:
                        self.seed_connections[remote_seed] = clientSocket
                        if remote_seed not in self.known_seeds:
                            self.known_seeds.append(remote_seed)
                        self.log("Seed connection accepted from " + str(remote_seed))
                        reply = f"I am seed|{(self.ip, self.port)}\n"
                        clientSocket.sendall(reply.encode())
                        clientSocket.settimeout(None)
                        time.sleep(0.1)
                        heartbeat = f"Heartbeat from {(self.ip, self.port)}\n"
                        clientSocket.sendall(heartbeat.encode())
                        threading.Thread(target=self.handle_client, args=(clientSocket,), daemon=True).start()
                    else:
                        self.log("Duplicate seed connection from " + str(remote_seed))
                        clientSocket.close()
                    continue

                try:
                    new_peer = ast.literal_eval(handshake)
                except Exception as e:
                    self.log("Error parsing peer handshake, closing connection. " + str(e))
                    clientSocket.close()
                    continue

                self.log("Peer connection accepted from: " + str(new_peer))
                if self.peer_connections.addNeighbour(new_peer[0], new_peer[1], clientSocket):
                    time.sleep(1)
                    connected_seeds = self.get_seeds_connected_to(new_peer)
                    if self.is_my_turn(new_peer, connected_seeds):
                        subset = self.get_peer_subset()
                        clientSocket.sendall(pickle.dumps(subset) + b"\n")
                        self.broadcastNewNodeUpdate(new_peer, subset)
                    else:
                        subset = []
                        clientSocket.sendall(pickle.dumps(subset) + b"\n")
                    self.updatePeerConnections(new_peer, subset)
                    clientSocket.settimeout(None)
                    threading.Thread(target=self.handle_client, args=(clientSocket,), daemon=True).start()
                else:
                    self.log("Duplicate peer connection from " + str(new_peer))
                    clientSocket.close()
            except Exception as e:
                self.log("Error accepting connection: " + str(e))
                continue

    def connectToSeed(self, seed):
        ip, port = seed
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((ip, port))
            handshake = f"I am seed|{(self.ip, self.port)}\n"
            sock.sendall(handshake.encode())
            f = sock.makefile('r')
            reply = f.readline().strip()
            if reply.startswith("I am seed"):
                try:
                    parts = reply.split("|")
                    if len(parts) != 2:
                        raise Exception("Malformed handshake reply")
                    remote_seed = ast.literal_eval(parts[1].strip())
                except Exception as e:
                    self.log("Error parsing handshake reply: " + str(e))
                    sock.close()
                    return
                sock.settimeout(None)
                self.seed_connections[remote_seed] = sock
                if remote_seed not in self.known_seeds:
                    self.known_seeds.append(remote_seed)
                self.log("Connected to seed " + str(remote_seed))
                time.sleep(0.1)
                heartbeat = f"Heartbeat from {(self.ip, self.port)}\n"
                sock.sendall(heartbeat.encode())
                threading.Thread(target=self.handle_client, args=(sock,), daemon=True).start()
            else:
                self.log("Unexpected handshake reply from seed " + str((ip, port)))
                sock.close()
        except Exception as e:
            self.log("Could not connect to seed " + str(seed) + " error: " + str(e))

    def connectToSeedsFromConfig(self):
        while self.running:
            for seed in self.known_seeds:
                if seed not in self.seed_connections:
                    threading.Thread(target=self.connectToSeed, args=(seed,), daemon=True).start()
            time.sleep(15)

    def broadcastMessage(self, message):
        self.log("Broadcasting message: " + message.strip())
        for seed, sock in list(self.seed_connections.items()):
            try:
                sock.sendall(message.encode())
            except Exception as e:
                self.log(f"Error sending to seed {seed}: {e}")
                self.seed_connections.pop(seed, None)

    def periodicBroadcast(self):
        while self.running:
            message = f"Heartbeat from {(self.ip, self.port)}\n"
            self.broadcastMessage(message)
            time.sleep(15)

    def removeDeadNode(self, deadMessage) -> None:
        deadMessage = deadMessage.strip()
        parts = deadMessage.split(":", 1)
        if parts[0] == "Dead Node":
            try:
                reported = ast.literal_eval(parts[1].strip())
            except Exception as e:
                self.log("Error parsing dead node message: " + str(e))
                return
            # Look up the peer in network_topology by matching both IP and port.
            found = None
            for peer in list(self.network_topology.keys()):
                if peer[0] == reported[0] and peer[1] == reported[1]:
                    found = peer
                    break
            if found is None:
                self.log("Dead node " + str(reported) + " not found in network topology; no broadcast sent.")
                return

            deadNode = found

            # Remove from peer_connections.
            removed = self.peer_connections.removeNeighbour(deadNode[0], deadNode[1])
            if removed:
                self.log("Removed dead node from peer_connections: " + str(deadNode))
            else:
                self.log("Dead node " + str(deadNode) + " not found in peer_connections; proceeding with removal.")

            # Completely remove deadNode from network_topology.
            if deadNode in self.network_topology:
                del self.network_topology[deadNode]
            for node in list(self.network_topology.keys()):
                if deadNode in self.network_topology[node]:
                    self.network_topology[node].discard(deadNode)

            # Also remove from known_peers.
            if deadNode in self.known_peers:
                self.known_peers.remove(deadNode)

            self.log("Completely removed dead node: " + str(deadNode))
            self.broadcastMessage(f"Dead Node: {deadNode}\n")

            # Also remove from known_peers.
            if deadNode in self.known_peers:
                self.known_peers.remove(deadNode)

            self.log("Completely removed dead node: " + str(deadNode))
            # Broadcast a clean dead node message.
            self.broadcastMessage(f"Dead Node: {deadNode}\n")

    def writeLog(self, content):
        try:
            with open(self.log_file, 'a') as file:
                file.write(content + "\n")
        except Exception as e:
            print("Error writing log to file:", e)

    def handle_client(self, client_socket):
        try:
            peer_addr = client_socket.getpeername()
        except Exception as e:
            peer_addr = "unknown"
        f = client_socket.makefile('r')
        while self.running:
            try:
                raw_data = f.readline()
                if not raw_data:
                    time.sleep(5)
                    continue
                for line in raw_data.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    self.log(f"Received message from {peer_addr}: {line}")
                    if line.startswith("NewNodeUpdate|"):
                        self.handleNewNodeUpdate(line)
                    elif line.startswith("Dead Node"):
                        try:
                            self.removeDeadNode(line)
                            self.log(f"Processed dead node message from {peer_addr}: {line}")
                        except Exception as e:
                            self.log(f"Corrupted dead node message from {peer_addr}: {line}; Error: {e}")
                    else:
                        self.log(f"Unrecognized message from {peer_addr}: {line}")
            except Exception as e:
                self.log(f"Corrupted message from {peer_addr}: {raw_data if 'raw_data' in locals() else 'unknown'}; Error: {e}")
                break

    def acceptInput(self):
        while self.running:
            user_input = input().strip()
            if user_input.lower() == "exit":
                self.log("Shutting down the seed node...")
                self.running = False
                self.socket.close()
                break
            else:
                self.log("Unrecognized command. Use 'exit' to shut down.")

    def activate(self):
        threading.Thread(target=self.acceptConnection, daemon=True).start()
        threading.Thread(target=self.connectToSeedsFromConfig, daemon=True).start()
        threading.Thread(target=self.periodicBroadcast, daemon=True).start()
        threading.Thread(target=self.acceptInput, daemon=True).start()

    def printPeerConnections(self):
        self.log("Current Regular Peer Connections:")
        for peer_tuple, peer_obj in self.peer_connections.neighbour.items():
            self.log(str(peer_obj))
        self.log("Current Seed Connections:")
        for seed in self.seed_connections:
            self.log(str(seed))
        self.log("Known Seeds: " + str(self.known_seeds))
        self.log("Known Peers (non-seed): " + str(self.known_peers))
        self.log("Network Topology:")
        self.log(str(self.network_topology))

    def __del__(self):
        if self.socket:
            self.socket.close()

def main():
    ip = "127.0.0.1"
    port = int(input("Enter Seed Port Number: "))
    seed = SeedNode(ip, port)
    seed.setup()
    seed.activate()
    while seed.running:
        time.sleep(30)
        seed.printPeerConnections()
    print("Final Network Topology:")
    print(seed.network_topology)

if __name__ == "__main__":
    main()
