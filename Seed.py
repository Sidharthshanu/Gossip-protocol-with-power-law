import socket
import pickle
import threading
import time

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


class peerNodeConnections:
    """
    Manages the local node's connected peers.
    Instead of storing just sockets, it stores Peer instances,
    which include extra information (like the set of connections).
    """
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.running = True
        # Dictionary mapping (ip, port) to a Peer instance.
        self.neighbour = {}
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
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.socket = None
        self.running = True
        # Use peerNodeConnections to manage direct peer connections.
        self.peer_connections = peerNodeConnections(ip, port)
        # List of known seeds (tuples) loaded from config.txt.
        self.known_seeds = []
        # A dictionary to maintain topology updates received via broadcast.
        self.network_topology = {}
        self.loadConfig()

    def loadConfig(self):
        """
        Loads known seeds from config.txt.
        Each line should be in the format ip:port.
        Skips the node's own address.
        """
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
                    print("Error parsing line in config.txt:", line, e)
        except Exception as e:
            print("Failed to open config.txt:", e)

    def get_peer_subset(self):
        """
        Returns a subset of the current peer connections.
        For example, if there are more than 3 connections, returns just the first three.
        This list (of (ip, port) tuples) will be sent during the handshake.
        """
        keys = list(self.peer_connections.neighbour.keys())
        if len(keys) > 3:
            return keys[:3]
        return keys

    def updatePeerConnections(self, new_peer, subset):
        """
        Updates the connection lists when a new connection is established.
        For the new peer, adds all the peers in 'subset' as its connections.
        For each peer in 'subset' that is already known locally, adds the new peer to that peer's connections.
        Also updates the network_topology dictionary.
        """
        self.network_topology[new_peer] = subset
        if new_peer in self.peer_connections.neighbour:
            peer_instance = self.peer_connections.neighbour[new_peer]
            for p in subset:
                peer_instance.add_connection(p)
                if p in self.peer_connections.neighbour:
                    self.peer_connections.neighbour[p].add_connection(new_peer)

    def get_seeds_connected_to(self, new_peer):
        """
        Checks which connected seeds (from our direct connections)
        have already recorded the new peer in their connections.
        Returns a list of (ip, port) tuples.
        """
        connected = []
        for seed, peer_obj in self.peer_connections.neighbour.items():
            if new_peer in peer_obj.connections:
                connected.append(seed)
        return connected

    def is_my_turn(self, new_peer, connected_seeds):
        """
        Determines if this node should send its connection list to the new peer.
        It is given:
          • new_peer: the (ip, port) tuple of the new peer,
          • connected_seeds: a list of seeds (each as (ip, port)) that have connected to the new peer.
        The function computes a hash from new_peer's ip+port, modulated by the total number of candidates.
        Candidates is defined as the ordered (sorted) list of seeds that have connected to the new peer plus self.
        If the candidate at the computed index equals this node's own address, returns True; otherwise, False.
        """
        candidates = connected_seeds.copy()
        self_tuple = (self.ip, self.port)
        if self_tuple not in candidates:
            candidates.append(self_tuple)
        candidates.sort(key=lambda x: (x[0], x[1]))
        hash_input = new_peer[0] + str(new_peer[1])
        index = hash(hash_input) % len(candidates)
        return candidates[index] == self_tuple

    def broadcastNewNodeUpdate(self, new_peer, subset):
        """
        Broadcasts a topology update to all connected seeds.
        The message contains the new node's address and the connection list (subset) that is being sent.
        """
        # Use a pipe-delimited format: "NewNodeUpdate|<new_peer>|<subset>"
        msg = f"NewNodeUpdate|{new_peer}|{subset}"
        print("Broadcasting new node update:", msg)
        self.broadcastMessage(msg)

    def handleNewNodeUpdate(self, msg):
        """
        Handles an incoming broadcast topology update.
        Expects a message in the format: "NewNodeUpdate|<new_peer>|<subset>"
        Parses the message and updates the local network_topology.
        """
        try:
            parts = msg.split("|")
            if len(parts) != 3:
                print("Malformed NewNodeUpdate message:", msg)
                return
            new_peer = eval(parts[1])
            subset = eval(parts[2])
            # Update the local topology view.
            self.network_topology[new_peer] = subset
            if new_peer not in self.known_seeds:
                self.known_seeds.append(new_peer)
            print("Updated topology with new node:", new_peer, "connections:", subset)
        except Exception as e:
            print("Error handling NewNodeUpdate message:", e)

    def setup(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))
        self.socket.listen()
        print(f"SeedNode is listening on {self.ip}:{self.port}")

    def acceptConnection(self) -> None:
        """
        Accepts incoming connections from other seeds.
        Handshake protocol:
          1. The connecting peer immediately sends its address.
          2. This node adds the connection and stalls for 1 second.
          3. It then checks which other seeds have connected to the same new peer.
          4. It calls is_my_turn() with that list.
             - If True, it gets its connection subset (via get_peer_subset) and sends it.
             - Also, it broadcasts a NewNodeUpdate message to all connected seeds.
             - Otherwise, it sends an empty list.
          5. Finally, it updates its local connection lists.
        """
        while self.running:
            try:
                clientSocket, clientAddress = self.socket.accept()
                # Step 1: Immediately receive the new peer's address.
                data = clientSocket.recv(1024).decode()
                new_peer = eval(data)  # Expected as a tuple, e.g. ('127.0.0.1', 8000)
                print("New connection from:", new_peer)
                if new_peer not in self.known_seeds:
                    self.known_seeds.append(new_peer)
                # Step 2: Add the new connection.
                if self.peer_connections.addNeighbour(new_peer[0], new_peer[1], clientSocket):
                    output = "Connection accepted from " + str(new_peer)
                    print(output)
                    self.writeLog(output)
                    # Stall for 1 second.
                    time.sleep(1)
                    # Step 3: Check which seeds already have this new peer.
                    connected_seeds = self.get_seeds_connected_to(new_peer)
                    # Step 4: Determine if it is this node’s turn.
                    if self.is_my_turn(new_peer, connected_seeds):
                        subset = self.get_peer_subset()
                        # Send the connection list to the new peer.
                        clientSocket.send(pickle.dumps(subset))
                        # Additionally, broadcast the new node update so that all seeds update their topology.
                        self.broadcastNewNodeUpdate(new_peer, subset)
                    else:
                        subset = []
                        clientSocket.send(pickle.dumps(subset))
                    # Step 5: Update local connection lists.
                    self.updatePeerConnections(new_peer, subset)
                    threading.Thread(target=self.handle_client, args=(clientSocket,)).start()
                else:
                    output = "Duplicate connection from " + str(new_peer)
                    print(output)
                    self.writeLog(output)
                    clientSocket.close()
            except Exception as e:
                print("Error accepting connection:", e)
                continue

    def connectToSeedsFromConfig(self):
        """
        Periodically tries to connect to each known seed (from config and discovered)
        that isn't already connected.
        For an outgoing connection, the protocol is:
          1. Immediately send this node's address.
          2. Wait to receive the connection list (which may be empty) from the remote seed.
          3. Update local connection lists accordingly.
        """
        while self.running:
            for seed in self.known_seeds:
                if seed not in self.peer_connections.neighbour:
                    ip, port = seed
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        sock.connect((ip, port))
                        # Immediately send our address.
                        sock.send(str((self.ip, self.port)).encode())
                        # Wait for the remote seed to decide which connection list to send.
                        data = sock.recv(1024)
                        remote_subset = pickle.loads(data)
                        print(f"Received peer subset from seed {(ip, port)}: {remote_subset}")
                        if self.peer_connections.addNeighbour(ip, port, sock):
                            output = f"Connected to seed {(ip, port)}"
                            print(output)
                            self.writeLog(output)
                            # Update local connection lists based on the remote subset.
                            self.updatePeerConnections((ip, port), remote_subset)
                            threading.Thread(target=self.handle_client, args=(sock,)).start()
                        else:
                            print("Duplicate connection to", (ip, port))
                            sock.close()
                    except Exception as e:
                        print("Could not connect to seed", (ip, port), "error:", e)
                        continue
            time.sleep(10)

    def broadcastMessage(self, message):
        """
        Broadcasts a message to all connected peers.
        This function is independent and can be called programmatically.
        """
        print("Broadcasting message:", message)
        for peer, peer_obj in list(self.peer_connections.neighbour.items()):
            try:
                peer_obj.socket.send(message.encode())
            except Exception as e:
                print(f"Error sending to {peer}: {e}")
                self.peer_connections.removeNeighbour(peer[0], peer[1])

    def periodicBroadcast(self):
        """
        Periodically broadcasts a heartbeat message.
        Adjust the message and interval as needed.
        """
        while self.running:
            message = f"Heartbeat from {(self.ip, self.port)}"
            self.broadcastMessage(message)
            time.sleep(15)

    def removeDeadNode(self, deadMessage) -> None:
        """
        Removes a dead node when a message of the form "Dead Node:(ip, port)" is received.
        """
        messageParts = deadMessage.split(":")
        if messageParts[0] == "Dead Node":
            deadNode = eval(messageParts[1])
            if self.peer_connections.removeNeighbour(deadNode[0], deadNode[1]):
                output = "Removed dead node: " + str(deadNode)
                print(output)
                self.writeLog(output)

    def writeLog(self, content):
        """
        Logs the provided content to a file named seed_log_<port>.txt.
        """
        file_path = "seed_log_" + str(self.port) + '.txt'
        with open(file_path, 'a') as file:
            file.write(content + '\n')

    def handle_client(self, client_socket):
        """
        Handles communication with a connected peer.
        Receives messages and processes them. Also checks for NewNodeUpdate messages
        to update the local topology.
        """
        while self.running:
            try:
                message = client_socket.recv(1024).decode()
                if not message:
                    client_socket.close()
                    break
                # If the message is a topology update, process it.
                if message.startswith("NewNodeUpdate|"):
                    self.handleNewNodeUpdate(message)
                else:
                    self.removeDeadNode(message)
                    print("Received message:", message)
                    self.writeLog(message)
            except Exception as e:
                print("Error in handle_client:", e)
                break

    def acceptInput(self):
        """
        Accepts user input solely for shutdown purposes.
        (Broadcasting is handled automatically.)
        """
        while self.running:
            user_input = input().strip()
            if user_input.lower() == "exit":
                print("Shutting down the seed node...")
                self.running = False
                self.socket.close()
                break
            else:
                print("Unrecognized command. Use 'exit' to shut down.")

    def activate(self):
        # Start thread to accept incoming connections.
        threading.Thread(target=self.acceptConnection).start()
        # Start thread to periodically connect to seeds.
        threading.Thread(target=self.connectToSeedsFromConfig).start()
        # Start thread for periodic broadcast independent of user input.
        threading.Thread(target=self.periodicBroadcast).start()
        # Start thread to accept shutdown commands from the keyboard.
        threading.Thread(target=self.acceptInput).start()

    def printPeerConnections(self):
        """
        Optional helper to print all current peer connections (for debugging).
        """
        print("Current Peer Connections:")
        for peer_tuple, peer_obj in self.peer_connections.neighbour.items():
            print(peer_obj)

    def __del__(self):
        if self.socket:
            self.socket.close()


def main():
    ip = "127.0.0.1"
    port = int(input("Enter Seed Port Number : "))
    seed = SeedNode(ip, port)
    seed.setup()
    seed.activate()
    # For debugging: periodically print the current peer connections.
    while seed.running:
        time.sleep(30)
        seed.printPeerConnections()


if __name__ == "__main__":
    main()
