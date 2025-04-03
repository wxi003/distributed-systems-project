import grpc
import raft_pb2
import time
import raft_pb2_grpc
# import sendMessage_pb2
# import sendMessage_pb2_grpc
from typing import List, Optional

class RaftClient:
    def __init__(self, serverPorts: List[str]):
        self.serverPorts = serverPorts
        self.currentLeader = None 

    def findLeader(self) -> Optional[str]:
        for port in self.serverPorts:
            try:
                with grpc.insecure_channel("localhost:" + port) as channel:
                    stub =raft_pb2_grpc.RaftServiceStub(channel)
                    request = raft_pb2.CheckLeaderRequest(clientID="client1")
                    response = stub.CheckLeader(request)  # Set a timeout
                    if response.isLeader:
                        print(f"Found leader at {port}")
                        self.currentLeader = port
                        return port
            except grpc.RpcError as e:
                print(f"Failed to connect to {port}: {e}")
                continue
        print("No leader found!")
        return None

    def sendRequest(self, requestData: str):
        #no current leader, try to find the leader
        if not self.currentLeader:
            self.currentLeader = self.findLeader()
        #traverse the array, still can not find the leader
        if not self.currentLeader:
            raise Exception("No leader available")

        try:
            with grpc.insecure_channel("localhost:" + str(self.currentLeader)) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                time.sleep(1)
                request = raft_pb2.SendMessageRequest(message=requestData)
                response = stub.SendMessage(request)
                if not response.isLeader:
                    print("Leader changed, rediscovering...")
                    self.currentLeader = self.findLeader()
                    return self.sendRequest(requestData)
                if response.isSuccessful:
                    print(f"Message was successfully sent.")
                else:
                    print(f"Failed to send the message.")              
        except grpc.RpcError as e:
            print(f"Error communicating with {self.currentLeader}: {e}")
            self.currentLeader = None  # Reset current leader to none, try to find leader
            return self.sendRequest(requestData)

if __name__ == "__main__":
    serverPorts = []
    while True:
        newPort = input("Please enter server port, enter q to finish: ")
        if newPort != "q":
            serverPorts.append(newPort)
        else:
            break
    serverPorts = list(set(serverPorts))
    client = RaftClient(serverPorts)
    while True:
        newMessage = input("Please enter your message: ")
        client.sendRequest(newMessage)
        print("")