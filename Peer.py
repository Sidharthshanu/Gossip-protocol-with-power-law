#!/usr/bin/env python3
import socket
import pickle
import threading
import time
import sys
import ast  # for safe literal evaluation
import queue
import datetime
import math

class PeerNode:
    def __init__(self, my_ip, my_port, config_file="config.txt"):
        self.my_ip = my_ip
        self.my_port = my_port
        self.config_file = config_file
        # Dictionary for seed connections: keys: (seed_ip, seed_port), values: (socket, writer)
        self.seed_connections = {}
        self.seed_queue = queue.Queue()  # Outgoing messages to seeds
        self.running = True
        self.silent = False  # if True, this peer stops sending heartbeats/replies

        # For processing the subset received from seeds: only the first one is used.
        self.subset_received = False
        self.first_subset = None

        # Dictionaries for peer connections and heartbeat timestamps.
        self.peer_connections = {}                # {(ip, port): socket}
        self.incoming_peer_connections = {}       # {(ip, port): socket}
        self.peer_last_heartbeat = {}             # outgoing connections
        self.incoming_peer_last_heartbeat = {}     # incoming connections

        # Dictionary storing reported identity from heartbeat messages.
        # Key: peer connection key; Value: reported identity (tuple)
        self.reported_identity = {}

        self.log_file = f"peer_log_{self.my_port}.txt"
        self.log(f"Peer started on {self.my_ip}:{self.my_port}")

    def log(self, message):
        """Logs a message with a timestamp to stdout and to a file."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - {message}"
        print(log_message)
        try:
            with open(self.log_file, "a") as f:
                f.write(log_message + "\n")
        except Exception as e:
            print("Error writing log:", e)

    def load_all_seeds_from_config(self):
        seeds = []
        try:
            with open(self.config_file, "r") as f:
                lines = f.readlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    ip, port_str = line.split(":")
                    port = int(port_str)
                    # Skip self
                    if ip == self.my_ip and port == self.my_port:
                        continue
                    seeds.append((ip, port))
                except Exception as e:
                    self.log(f"Error parsing config line '{line}': {e}")
        except Exception as e:
            self.log(f"Error reading config file: {e}")
            sys.exit(1)
        return seeds

    def connect_to_multiple_seeds(self):
        seeds = self.load_all_seeds_from_config()
        if not seeds:
            self.log("No seeds found in config file!")
            sys.exit(1)
        # Compute half+1 seeds.
        num_to_connect = math.floor(len(seeds) / 2) + 1
        seeds_to_connect = seeds[:num_to_connect]
        self.log(f"Connecting to seeds (half+1): {seeds_to_connect}")
        for seed in seeds_to_connect:
            threading.Thread(target=self.connect_to_seed, args=(seed,), daemon=True).start()

    def connect_to_seed(self, seed):
        try:
            seed_ip, seed_port = seed
            self.log(f"Connecting to seed {seed_ip}:{seed_port} ...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((seed_ip, seed_port))
            sock.settimeout(None)
            writer = sock.makefile("w")
            handshake = str((self.my_ip, self.my_port)) + "\n"
            writer.write(handshake)
            writer.flush()
            self.log(f"Sent handshake to seed {seed_ip}:{seed_port}: {handshake.strip()}")
            data = sock.recv(4096)
            try:
                # Process subset only if not already received.
                if not self.subset_received:
                    subset = pickle.loads(data)
                    if self.first_subset is None:
                        self.first_subset = subset
                        self.log(f"First peer subset received from seed {seed}: {subset}")
                        # Delay processing by 1 second to ensure the first subset is used.
                        threading.Timer(1.0, self.process_first_subset).start()
                    else:
                        self.log(f"Ignoring peer subset from seed {seed} (first subset already saved)")
                else:
                    self.log(f"Ignoring peer subset from seed {seed} (subset already processed)")
            except Exception as e:
                self.log(f"Received message from seed {seed}: {data.decode().strip()}")
            self.seed_connections[seed] = (sock, writer)
            threading.Thread(target=self.handle_seed_incoming, args=(seed, sock), daemon=True).start()
        except Exception as e:
            self.log(f"Error connecting to seed {seed}: {e}")

    def process_first_subset(self):
        if not self.subset_received and self.first_subset is not None:
            self.log(f"Processing first received subset: {self.first_subset}")
            self.connect_to_peers(self.first_subset)
            self.subset_received = True
            # Start gossip message generation after registration is complete.
            threading.Thread(target=self.gossip_sender, daemon=True).start()

    def seed_sender(self):
        """Continuously send queued messages to all connected seeds."""
        while self.running:
            try:
                msg = self.seed_queue.get()
                if not msg.endswith("\n"):
                    msg += "\n"
                for seed, (sock, writer) in list(self.seed_connections.items()):
                    try:
                        writer.write(msg)
                        writer.flush()
                        self.log(f"Sent to seed {seed}: {msg.strip()}")
                    except Exception as e:
                        self.log(f"Error sending to seed {seed}: {e}")
                        self.seed_connections.pop(seed, None)
            except Exception as e:
                self.log(f"Error in seed_sender: {e}")
            time.sleep(0.1)

    def send_to_seed(self, message):
        if not message.endswith("\n"):
            message += "\n"
        self.seed_queue.put(message)
        self.log(f"Enqueued message to seed: {message.strip()}")

    def handle_seed_incoming(self, seed, sock):
        while self.running:
            try:
                data = sock.recv(1024)
                if not data:
                    self.log(f"Seed connection {seed} closed.")
                    self.seed_connections.pop(seed, None)
                    break
                try:
                    subset = pickle.loads(data)
                    self.log(f"Received updated peer subset from seed {seed}: {subset}")
                    self.connect_to_peers(subset)
                except Exception:
                    message = data.decode().strip()
                    self.log(f"Received message from seed {seed}: {message}")
            except Exception as e:
                self.log(f"Error receiving from seed {seed}: {e}")
                self.seed_connections.pop(seed, None)
                break

    def start_peer_server(self):
        """Starts a server to accept incoming peer connections."""
        def handle_peer_client(conn, addr):
            peer_addr = (addr[0], addr[1])
            self.incoming_peer_connections[peer_addr] = conn
            self.incoming_peer_last_heartbeat[peer_addr] = time.time()
            self.log(f"[Peer Server] Incoming connection accepted from {peer_addr}")
            while self.running:
                try:
                    data = conn.recv(1024)
                    if not data:
                        self.log(f"[Peer Server] Connection closed from {peer_addr}")
                        conn.close()
                        self.incoming_peer_connections.pop(peer_addr, None)
                        self.incoming_peer_last_heartbeat.pop(peer_addr, None)
                        break
                    message = data.decode()
                    if message.startswith("Heartbeat"):
                        try:
                            parts = message.split("from")
                            if len(parts) > 1:
                                reported = ast.literal_eval(parts[1].strip())
                                self.reported_identity[peer_addr] = reported
                            else:
                                self.reported_identity[peer_addr] = peer_addr
                        except Exception as e:
                            self.reported_identity[peer_addr] = peer_addr
                        self.incoming_peer_last_heartbeat[peer_addr] = time.time()
                    elif message.strip() == "PING":
                        if not self.silent:
                            heartbeat = f"Heartbeat from {(self.my_ip, self.my_port)}\n"
                            conn.sendall(heartbeat.encode())
                            self.incoming_peer_last_heartbeat[peer_addr] = time.time()
                    self.log(f"[Peer Server] Message from {peer_addr}: {message.strip()}")
                except Exception as e:
                    self.log(f"[Peer Server] Error with connection {peer_addr}: {e}")
                    try:
                        conn.close()
                    except:
                        pass
                    self.incoming_peer_connections.pop(peer_addr, None)
                    self.incoming_peer_last_heartbeat.pop(peer_addr, None)
                    self.reported_identity.pop(peer_addr, None)
                    break

        def server_thread():
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_sock.bind((self.my_ip, self.my_port))
            server_sock.listen()
            self.log(f"[Peer Server] Listening on {self.my_ip}:{self.my_port} for peer connections")
            while self.running:
                try:
                    conn, addr = server_sock.accept()
                    threading.Thread(target=handle_peer_client, args=(conn, addr), daemon=True).start()
                except Exception as e:
                    self.log(f"[Peer Server] Error accepting connection: {e}")
                    break
            server_sock.close()
        threading.Thread(target=server_thread, daemon=True).start()

    def connect_to_peers(self, peer_list):
        for peer in peer_list:
            if peer == (self.my_ip, self.my_port):
                continue
            if peer in self.peer_connections:
                continue
            threading.Thread(target=self.connect_to_peer, args=(peer,), daemon=True).start()

    def connect_to_peer(self, peer):
        ip, port = peer
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((ip, port))
            s.settimeout(None)
            self.peer_connections[peer] = s
            self.peer_last_heartbeat[peer] = time.time()
            self.log(f"Connected to peer {peer}")
            if not self.silent:
                heartbeat = f"Heartbeat from {(self.my_ip, self.my_port)}\n"
                s.sendall(heartbeat.encode())
            threading.Thread(target=self.handle_peer_connection, args=(s, peer), daemon=True).start()
        except Exception as e:
            self.log(f"Error connecting to peer {peer}: {e}")

    def handle_peer_connection(self, sock, peer):
        while self.running:
            try:
                data = sock.recv(1024)
                if not data:
                    self.log(f"Connection to peer {peer} closed.")
                    sock.close()
                    self.peer_connections.pop(peer, None)
                    self.peer_last_heartbeat.pop(peer, None)
                    self.reported_identity.pop(peer, None)
                    break
                message = data.decode()
                if message.startswith("Heartbeat"):
                    try:
                        parts = message.split("from")
                        if len(parts) > 1:
                            reported = ast.literal_eval(parts[1].strip())
                            self.reported_identity[peer] = reported
                        else:
                            self.reported_identity[peer] = peer
                    except Exception as e:
                        self.reported_identity[peer] = peer
                    self.peer_last_heartbeat[peer] = time.time()
                elif message.strip() == "PING":
                    if not self.silent:
                        heartbeat = f"Heartbeat from {(self.my_ip, self.my_port)}\n"
                        sock.sendall(heartbeat.encode())
                        self.peer_last_heartbeat[peer] = time.time()
                self.log(f"Message from peer {peer}: {message.strip()}")
            except Exception as e:
                self.log(f"Error in connection with peer {peer}: {e}")
                try:
                    sock.close()
                except:
                    pass
                self.peer_connections.pop(peer, None)
                self.peer_last_heartbeat.pop(peer, None)
                self.reported_identity.pop(peer, None)
                break

    def monitor_peer_heartbeats(self):
        HEARTBEAT_TIMEOUT = 30  # seconds
        PING_WAIT = 2           # seconds to wait after ping
        while self.running:
            now = time.time()
            for peer, last in list(self.peer_last_heartbeat.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    self.log(f"No heartbeat from peer {peer} for {now - last:.1f} seconds. Pinging...")
                    try:
                        self.peer_connections[peer].sendall("PING\n".encode())
                        time.sleep(PING_WAIT)
                        if now - self.peer_last_heartbeat.get(peer, now) > HEARTBEAT_TIMEOUT:
                            reported_id = self.reported_identity.get(peer, peer)
                            dead_message = f"Dead Node: {reported_id}"
                            self.log(f"Peer {reported_id} appears dead. Reporting dead node to all seeds.")
                            self.send_to_seed(dead_message)
                            try:
                                self.peer_connections[peer].close()
                            except:
                                pass
                            self.peer_connections.pop(peer, None)
                            self.peer_last_heartbeat.pop(peer, None)
                            self.reported_identity.pop(peer, None)
                    except Exception as e:
                        self.log(f"Error pinging peer {peer}: {e}")
                        reported_id = self.reported_identity.get(peer, peer)
                        dead_message = f"Dead Node: {reported_id}"
                        self.send_to_seed(dead_message)
                        try:
                            self.peer_connections[peer].close()
                        except:
                            pass
                        self.peer_connections.pop(peer, None)
                        self.peer_last_heartbeat.pop(peer, None)
                        self.reported_identity.pop(peer, None)
            for peer, last in list(self.incoming_peer_last_heartbeat.items()):
                if now - last > HEARTBEAT_TIMEOUT:
                    self.log(f"No heartbeat from incoming peer {peer} for {now - last:.1f} seconds. Pinging...")
                    try:
                        self.incoming_peer_connections[peer].sendall("PING\n".encode())
                        time.sleep(PING_WAIT)
                        if now - self.incoming_peer_last_heartbeat.get(peer, now) > HEARTBEAT_TIMEOUT:
                            reported_id = self.reported_identity.get(peer, peer)
                            dead_message = f"Dead Node: {reported_id}"
                            self.log(f"Incoming peer {reported_id} appears dead. Reporting dead node to all seeds.")
                            self.send_to_seed(dead_message)
                            try:
                                self.incoming_peer_connections[peer].close()
                            except:
                                pass
                            self.incoming_peer_connections.pop(peer, None)
                            self.incoming_peer_last_heartbeat.pop(peer, None)
                            self.reported_identity.pop(peer, None)
                    except Exception as e:
                        self.log(f"Error pinging incoming peer {peer}: {e}")
                        reported_id = self.reported_identity.get(peer, peer)
                        dead_message = f"Dead Node: {reported_id}"
                        self.send_to_seed(dead_message)
                        try:
                            self.incoming_peer_connections[peer].close()
                        except:
                            pass
                        self.incoming_peer_connections.pop(peer, None)
                        self.incoming_peer_last_heartbeat.pop(peer, None)
                        self.reported_identity.pop(peer, None)
            time.sleep(10)

    def periodic_peer_heartbeat(self):
        while self.running:
            if not self.silent:
                heartbeat = f"Heartbeat from {(self.my_ip, self.my_port)}\n"
                for peer, sock in list(self.peer_connections.items()):
                    try:
                        sock.sendall(heartbeat.encode())
                    except Exception as e:
                        self.log(f"Error sending heartbeat to peer {peer}: {e}")
                        try:
                            sock.close()
                        except:
                            pass
                        self.peer_connections.pop(peer, None)
                        self.peer_last_heartbeat.pop(peer, None)
                        self.reported_identity.pop(peer, None)
                for peer, sock in list(self.incoming_peer_connections.items()):
                    try:
                        sock.sendall(heartbeat.encode())
                    except Exception as e:
                        self.log(f"Error sending heartbeat to incoming peer {peer}: {e}")
                        try:
                            sock.close()
                        except:
                            pass
                        self.incoming_peer_connections.pop(peer, None)
                        self.incoming_peer_last_heartbeat.pop(peer, None)
                        self.reported_identity.pop(peer, None)
            time.sleep(15)

    def gossip_sender(self):
        count = 1
        while count <= 10 and self.running:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"{timestamp}:{self.my_ip}:{count}\n"
            self.log(f"Generated gossip message: {message.strip()}")
            # Broadcast gossip message to all connected peers.
            for peer, sock in list(self.peer_connections.items()):
                try:
                    sock.sendall(message.encode())
                except Exception as e:
                    self.log(f"Error sending gossip message to peer {peer}: {e}")
            count += 1
            time.sleep(5)

    def run(self):
        self.start_peer_server()
        self.connect_to_multiple_seeds()
        threading.Thread(target=self.seed_sender, daemon=True).start()
        threading.Thread(target=self.periodic_peer_heartbeat, daemon=True).start()
        threading.Thread(target=self.monitor_peer_heartbeats, daemon=True).start()
        self.log("Peer node is running. Type 'exit' to quit or '1' to enter silent mode (no heartbeat or PING responses).")
        while self.running:
            user_input = input("")
            if user_input.lower() == "exit":
                self.running = False
                for seed, (sock, writer) in list(self.seed_connections.items()):
                    try:
                        sock.close()
                    except:
                        pass
                for s in list(self.peer_connections.values()):
                    try:
                        s.close()
                    except:
                        pass
                for s in list(self.incoming_peer_connections.values()):
                    try:
                        s.close()
                    except:
                        pass
                break
            elif user_input.strip() == "1":
                self.silent = True
                self.log("Silent mode activated. This peer will no longer send heartbeat or respond to PINGs.")
            else:
                try:
                    self.send_to_seed(user_input)
                except Exception as e:
                    self.log(f"Error sending message to seed: {e}")
                    self.running = False
                    break

    def printPeerConnections(self):
        self.log("Current Outgoing Peer Connections:")
        for peer in self.peer_connections:
            self.log(str(peer))
        self.log("Current Incoming Peer Connections:")
        for peer in self.incoming_peer_connections:
            self.log(str(peer))

if __name__ == "__main__":
    my_ip = "127.0.0.1"
    try:
        my_port = int(input("Enter your peer port: "))
    except Exception as e:
        print("Invalid port input:", e)
        sys.exit(1)
    peer = PeerNode(my_ip, my_port)
    threading.Thread(target=peer.monitor_peer_heartbeats, daemon=True).start()
    peer.run()
