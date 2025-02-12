import socket
import select
import pickle
import threading
import sys

class SeedNode:
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.socket = None
        self.peers = []
        self.connections = []
        self.running = True

    def setup(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))
        self.socket.listen()

    def acceptConnection(self) -> None:
        while self.running:
            try:
                clientSocket, clientAddress = self.socket.accept()
                clientSocket.send(pickle.dumps(self.peers))
                data = clientSocket.recv(1024).decode()
                print(data)
                clientAddress = eval(data)
                self.peers.append(clientAddress)
                self.connections.append(clientSocket)
                output = "Connection accepted from " + str(clientAddress)
                print(output)
                self.writeLog(output)
            except socket.timeout:
                continue

    def removeDeadNode(self, deadMessage) -> None:
        messageParts = deadMessage.split(":")
        if messageParts[0] == "Dead Node":
            deadNode = eval(messageParts[1])
            if deadNode in self.peers:
                self.peers.remove(deadNode)

    def writeLog(self, content):
        file_path = "seed_log_" + str(self.port) + '.txt'
        with open(file_path, 'a') as file:
            file.write(content + '\n')

    def handle_client(self, client_socket):
        while self.running:
            try:
                message = client_socket.recv(1024).decode()
                if not message:
                    self.connections.remove(client_socket)
                    client_socket.close()
                    continue
                self.removeDeadNode(message)
                print(message)
                self.writeLog(message)
            except:
                continue

    def activate(self):
        input_thread = threading.Thread(target=self.acceptInput)
        input_thread.start()

        for conn in self.connections:
            client_thread = threading.Thread(target=self.handle_client, args=(conn,))
            client_thread.start()

    def acceptInput(self):
        while self.running:
            user_input = input().strip()
            if user_input.lower() == "exit":
                print("Shutting down the seed node...")
                self.running = False
                self.socket.close()
                break

    def __del__(self):
        self.socket.close()

def main():
    ip = "127.0.0.1"
    port = int(input("Enter Seed Port Number : "))
    seed = SeedNode(ip, port)
    seed.setup()
    seed.activate()

if __name__ == "__main__":
    main()
