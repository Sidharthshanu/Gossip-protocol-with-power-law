import socket
import select
import random
import hashlib
from datetime import datetime
import time
import pickle
import threading
import errno

class Message:
    def __init__(self, message, sender) -> None:
        self.message = message
        self.sender = sender
        self.hash = hashlib.sha256(message.encode()).hexdigest()

class PeerNode:
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.listeningSocket = None
        self.seeds = []           # List of seed addresses (ip, port)
        self.peers = []           # List of peer addresses (ip, port)
        self.seedSockets = []     # Sockets connected to seeds
        self.peerConnections = [] # Sockets connected to peers
        self.messageList = []
        self.livenessStatus = {}
        self.sentMessageCount = 0
        self.lastGossipMessageTime = None
        self.lastLivenessMessageTime = None
        self.socketMapping = {}   # Maps socket to peer address
        self.allConnections = []
        self.running = True

    def setup(self) -> None:
        self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind((self.ip, self.port))
        self.listeningSocket.listen()
        self.listeningSocket.settimeout(1)  # Avoid blocking indefinitely
        self.allConnections.append(self.listeningSocket)

    def readConfigurations(self, filePath):
        seeds = []
        try:
            with open(filePath, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        seedIp, seedPort = line.split(':')
                        seeds.append((seedIp, int(seedPort)))
        except Exception as e:
            print(f"Error reading configurations: {e}")
        return seeds

    def establishConnections(self):
        # Connect to seeds only. Seeds are responsible for connecting peers.
        seeds = self.readConfigurations(filePath='config.txt')
        # Randomize the order so that connection attempts are spread out.
        seeds = random.sample(seeds, len(seeds))
        for seed in seeds:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(seed)
                # Seeds may send a peer subset for their own topology purposes.
                # As a peer, we ignore the received data.
                _ = pickle.loads(sock.recv(1024))
                self.seeds.append(seed)
                self.seedSockets.append(sock)
                # Simply send our address; no additional info is sent.
                sock.send(str((self.ip, self.port)).encode())
            except Exception as e:
                print(f"Connection to seed {seed} failed: {e}")

        # (Optionally) If the peer wants to connect to other peers directly,
        # that logic can be added here—but the seeds have the task of
        # connecting peers into the network.

    def acceptConnection(self):
        try:
            clientSocket, _ = self.listeningSocket.accept()
            # Immediately receive the actual address from the connecting node.
            data = clientSocket.recv(1024).decode()
            new_peer = eval(data)  # Expected format: (ip, port)
            print("New incoming connection from:", new_peer)
            if new_peer not in self.peers:
                self.peers.append(new_peer)
            if clientSocket not in self.peerConnections:
                self.peerConnections.append(clientSocket)
            self.socketMapping[clientSocket] = new_peer
            self.allConnections.append(clientSocket)
            # For simplicity, just send an acknowledgment.
            clientSocket.send("ACK".encode())
        except socket.timeout:
            pass

    def sendMessage(self, message, receivers):
        for sock in receivers:
            try:
                sock.send(message.encode())
            except Exception as e:
                print(f"Error sending message: {e}")

    def generateGossipMessage(self):
        timestamp = datetime.now().strftime("%H:%M:%S")
        messageText = f"Gossip Message {self.sentMessageCount + 1}"
        gossipMessage = f"{timestamp}:{str((self.ip, self.port))}:{messageText}"
        self.messageList.append(Message(gossipMessage, None))
        return gossipMessage

    def generateLivenessMessage(self):
        timestamp = datetime.now().strftime("%H:%M:%S")
        return f"Liveness Request:{timestamp}:{str((self.ip, self.port))}"

    def removeDeadNodes(self):
        deadPeers = [peer for peer, count in self.livenessStatus.items() if count > 3]
        for peer in deadPeers:
            self.livenessStatus.pop(peer, None)
            sock = self.socketMapping.pop(peer, None)
            if sock:
                if sock in self.allConnections:
                    self.allConnections.remove(sock)
                sock.close()
            if peer in self.peers:
                self.peers.remove(peer)

    def handleMessage(self, sock, message):
        messageParts = message.split(":")
        if messageParts[0] == "Liveness Request":
            senderTimestamp, senderIp = messageParts[1], messageParts[2]
            reply = f"Liveness Reply:{senderTimestamp}:{senderIp}:{str((self.ip, self.port))}"
            sock.send(reply.encode())
        elif messageParts[0] == "Liveness Reply":
            self.livenessStatus[eval(messageParts[3])] = 0
        else:
            # Handle gossip or other message types.
            print("Received message:", message)

    def acceptInput(self):
        while self.running:
            user_input = input("Type 'exit' to stop the node: ").strip()
            if user_input.lower() == 'exit':
                print("Shutting down the peer node...")
                self.running = False
                self.cleanup()
                break

    def cleanup(self):
        for sock in self.allConnections:
            sock.close()
        if self.listeningSocket:
            self.listeningSocket.close()

    def activate(self):
        input_thread = threading.Thread(target=self.acceptInput)
        input_thread.start()
        while self.running:
            try:
                self.allConnections = [s for s in self.allConnections if s.fileno() != -1]
                if not self.allConnections:
                    continue
                readable, _, _ = select.select(self.allConnections, [], [], 1)
                for sock in readable:
                    if sock == self.listeningSocket:
                        self.acceptConnection()
                    else:
                        message = sock.recv(1024).decode()
                        if not message:
                            if sock in self.allConnections:
                                self.allConnections.remove(sock)
                            sock.close()
                        else:
                            self.handleMessage(sock, message)
            except select.error as e:
                if e.args[0] not in (errno.EINTR, errno.EBADF):
                    raise

def main():
    ip = "127.0.0.1"
    port = int(input("Enter Peer Port Number: "))
    peer = PeerNode(ip, port)
    peer.setup()
    peer.establishConnections()
    peer.activate()

if __name__ == "__main__":
    main()
