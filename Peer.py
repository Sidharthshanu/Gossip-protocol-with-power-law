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
        self.seeds = []
        self.peers = []
        self.seedSockets = []
        self.peerConnections = []
        self.peerSockets = []
        self.messageList = []
        self.livenessStatus = {}
        self.sentMessageCount = 0
        self.lastGossipMessageTime = None
        self.lastLivenessMessageTime = None
        self.socketMapping = {}
        self.allConnections = []
        self.running = True

    def setup(self) -> None:
        self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind((self.ip, self.port))
        self.listeningSocket.listen()
        self.listeningSocket.settimeout(1)  # To avoid blocking indefinitely
        self.allConnections.append(self.listeningSocket)  # Make sure the listening socket is in the list

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
        seeds = self.readConfigurations(filePath='config.txt')
        seeds = random.sample(seeds, len(seeds))
        allPeers = []
        for seed in seeds:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(seed)
                peers = pickle.loads(sock.recv(1024))
                print("List of Peers:", peers)
                allPeers.extend(peers)
                self.seeds.append(seed)
                self.seedSockets.append(sock)
                sock.send(str((self.ip, self.port)).encode())
            except Exception as e:
                print(f"Connection to seed {seed} failed: {e}")

        allPeers = list(set(allPeers))
        for peer in allPeers:
            if len(self.peers) >= 4:
                break
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(peer)
                self.peers.append(peer)
                self.peerSockets.append(sock)
                self.socketMapping[sock] = peer
                sock.send(str((self.ip, self.port)).encode())
            except Exception as e:
                print(f"Connection to peer {peer} failed: {e}")

    def acceptConnection(self):
        try:
            clientSocket, clientAddress = self.listeningSocket.accept()
            self.peers.append(clientAddress)
            self.peerConnections.append(clientSocket)
            self.socketMapping[clientSocket] = clientAddress
            self.allConnections.append(clientSocket)
            print(f"New connection from {clientAddress}")
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
                self.allConnections.remove(sock)
                sock.close()
            self.peers.remove(peer)

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
                            self.allConnections.remove(sock)
                            sock.close()
                        else:
                            self.handleMessage(sock, message)

            except select.error as e:
                if e.args[0] not in (errno.EINTR, errno.EBADF):
                    raise

    def handleMessage(self, sock, message):
        messageParts = message.split(":")
        if messageParts[0] == "Liveness Request":
            senderTimestamp, senderIp = messageParts[1], messageParts[2]
            reply = f"Liveness Reply:{senderTimestamp}:{senderIp}:{str((self.ip, self.port))}"
            sock.send(reply.encode())
        elif messageParts[0] == "Liveness Reply":
            self.livenessStatus[eval(messageParts[3])] = 0
        else:
            # Gossip or other message handling
            pass

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
        self.listeningSocket.close()

def main():
    ip = "127.0.0.1"
    port = int(input("Enter Peer Port Number: "))
    peer = PeerNode(ip, port)
    peer.setup()
    peer.establishConnections()
    peer.activate()

if __name__ == "__main__":
    main()
