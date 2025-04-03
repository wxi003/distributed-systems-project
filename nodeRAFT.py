import threading
import time
import random
import grpc
import raft_pb2
import raft_pb2_grpc
import shutil
from concurrent import futures
from google.protobuf.empty_pb2 import Empty
import argparse
import sys

#Define roles as constants
ROLE_FOLLOWER = "Follower"  
ROLE_CANDIDATE = "Candidate"
ROLE_LEADER = "Leader"
class Server(raft_pb2_grpc.RaftServiceServicer):
    def replay(self, filename):
        with open(filename, 'r') as f:
            for line in f:
                parts = line.split(':', 1)
                if parts[0].isdigit():
                    self.log.append(int(parts[0]))
        

    def __init__(self, leader_port, follower_port):
        #Initialize a server with RAFT state
        if follower_port is not None:
            try:
                with grpc.insecure_channel("localhost:" + str(leader_port)) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    request = raft_pb2.ConnectLeaderRequest(secondary_port = follower_port)
                    response = stub.ConnectLeader(request, timeout=20)
                    print(response)
                    if response.status == raft_pb2.Status.FAILURE_NOT_LEADER:
                        print("The leader port is not current leader, please try again")
                        exit(0)
                    elif response.status == raft_pb2.Status.FAILURE_DUPLICATE_PORT:
                        print("The follower port is already listened by a Raft node, please try again")
                        exit(0)     
                    self.server_id = response.node_id
                    self.all_addresses = response.other_ports
                    self.log_file = response.log_file
                    self.log = []
                    self.replay(self.log_file)
                    self.next_node_id = len(response.other_ports) + 1
            except grpc.RpcError as e:
                print(f"Error communicating with leader: {e}")
                exit(0)
            self.role = ROLE_FOLLOWER
            self.port = follower_port
            self.leader = leader_port
        else:
            self.role = ROLE_LEADER
            self.port = leader_port
            self.leader = leader_port
            self.server_id = 0
            self.all_addresses = []
            self.log_file = "LOGNODE0.txt"
            self.next_node_id = 0
            self.leader_id = 0
            self.log = []
            with open(self.log_file, "w") as f:
                pass # Leave it empty for now
        self.next_index = 0
        self.term = 0
        self.voted_for = None
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(20, 50)
        self.votes_received = 0
        self.next_index = 0
        self.match_index = None
        self.last_heartbeat_sent = 0
        self.heartbeat_interval = 5
        self.lock = threading.Lock()
        # Start gRPC server
        self.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.grpc_server)
        self.grpc_server.add_insecure_port(f'[::]:{self.port}')
        self.grpc_server.start()
        print(f"Server {self.server_id} gRPC server started on port {self.port}")

    def run(self):
        #Main server loop: handle timeouts and initiate RPC calls
        while True:
            current_time = time.time()
            if self.role in [ROLE_FOLLOWER, ROLE_CANDIDATE]:
                if current_time - self.last_heartbeat_time > self.election_timeout:
                    print(f"Server {self.server_id} started an election.")
                    self.start_election()
            if self.role == ROLE_LEADER:
                if current_time - self.last_heartbeat_sent > self.heartbeat_interval:
                    self.send_heartbeats()
            time.sleep(1)  # Avoid busy waiting

    def start_election(self):
        #Start a leader election as a Candidate          
        self.role = ROLE_CANDIDATE
        self.term += 1
        self.voted_for = self.server_id
        self.votes_received = 1  # Vote for self
        self.last_heartbeat_time = time.time()
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1] if self.log else 0
        request = raft_pb2.RequestVoteRequest(
            term=self.term,
            candidate_id=self.server_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
                
        #Send RequestVote RPC to all other servers concurrently
        def send_request_vote(to_server_index):
            address = self.all_addresses[to_server_index]
            print(f"Server {self.server_id} sending request vote to {address}")
            with grpc.insecure_channel("localhost:" + str(address)) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                try:
                    response = stub.RequestVote(request)
                    if response.term > self.term:
                        self.term = response.term
                        self.voted_for = None
                        self.role = ROLE_FOLLOWER
                        return
                    if response.vote_granted and self.role == ROLE_CANDIDATE:
                        with self.lock:
                            self.votes_received += 1
                        if self.votes_received > len(self.all_addresses) // 2:
                            self.role = ROLE_LEADER
                            self.leader_id = self.server_id
                            print(f"Server {self.server_id} become leader")
                            self.send_heartbeats()
                except grpc.RpcError as e:
                    del self.all_addresses[to_server_index]
                    print(f"Server {self.server_id} RPC error to {address}: {e}")
        with futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(send_request_vote, [s for s in range(len(self.all_addresses))])

    def send_heartbeats(self):
        for server_port in self.all_addresses:
            request = raft_pb2.HeartBeat(
                leader_id=self.server_id
            )
            threading.Thread(target=self.send_heart_beat, args=(server_port,request)).start()
        self.last_heartbeat_sent = time.time()

    def send_heart_beat(self,server_port,request):
        with grpc.insecure_channel("localhost:" + str(server_port)) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                stub.DetectHeartBeats(request)
                print(f"Leader Server {self.server_id} HeartBeat to localhost::{server_port}")
            except grpc.RpcError as e:
                self.all_addresses.remove(server_port)
                print(f"Leader Server {self.server_id} HeartBeat error to localhost::{server_port}: {e}")

    def try_send_more_log(self, address, request):
        new_index = request.prev_log_index
        with open(self.log_file, "r") as f:
            for i, line in enumerate(f, start=0):
                if i == new_index:
                    current_line = line.split(":", 1)[1].strip()
                    current_term = line.split(":", 1)[0].strip()
        current_entry = raft_pb2.LogEntry(term = int(current_term), command = current_line)
        prev_log_term = self.log[new_index - 1] if new_index - 1 >= 0 else 0
        new_request = raft_pb2.AppendEntriesRequest(
                term=int(current_term),
                prev_log_index=new_index - 1,
                prev_log_term=prev_log_term,
                entry= current_entry
            )
        with grpc.insecure_channel("localhost:" + str(address)) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.AppendEntries(new_request)
                if response.success:
                    return True
                else:
                    if response.need_log and self.try_send_more_log(address, new_request):
                        response_new = stub.AppendEntries(request)
                        if response_new.success:
                            return True
                        else:
                            return False
                    else:
                        print("Follower reject Leader")
                        return False
            except grpc.RpcError as e:
                self.all_addresses.remove(address)
                print(f"Server {self.server_id} RPC error to port {address}: {e}")
                return False
            
    # Send logs to all follower, on success return 1, 0 otherwise
    def send_append_entries(self, address, request):
        with grpc.insecure_channel("localhost:" + str(address)) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.AppendEntries(request)
                if response.success:
                    return 1
                else:
                    if response.need_log and self.try_send_more_log(address, request):
                        stub.AppendEntries(request)
                        return 1
                    else:
                        print("Follower reject Leader")
                        return 0
            except grpc.RpcError as e:
                self.all_addresses.remove(address)
                print(f"Server {self.server_id} RPC error to port {address}: {e}")
                return 0

    def RequestVote(self, request, context):
        #Handle incoming RequestVote RPC
        if request.term > self.term:
            self.term = request.term
            self.voted_for = None
            self.role = ROLE_FOLLOWER
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1] if self.log else 0
        log_ok = (request.last_log_term > last_log_term) or \
                    (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
        if request.term >= self.term and (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok:
            self.voted_for = request.candidate_id
            self.last_heartbeat_time = time.time()
            print(f"Vote: Server {self.server_id} voted server {self.voted_for}")
            return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=True)
        return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=False)
    
    def ConnectLeader(self,request,context):
        if self.role != ROLE_LEADER:
            return raft_pb2.ConnectLeaderResponse(status = raft_pb2.Status.FAILURE_NOT_LEADER)
        for port in self.all_addresses:
            if port == request.secondary_port:
                return raft_pb2.ConnectLeaderResponse(status = raft_pb2.Status.FAILURE_DUPLICATE_PORT)
        self.next_node_id += 1
        new_log_file = "LOGNODE" + str(self.next_node_id) + ".txt"
        shutil.copy(self.log_file, new_log_file)
        response = raft_pb2.ConnectLeaderResponse(status = raft_pb2.Status.SUCCESS, 
                                                    node_id = self.next_node_id,
                                                    leader_id = self.server_id,
                                                    other_ports = self.all_addresses,
                                                    log_file = new_log_file)
        for server_port in self.all_addresses:
            requestb = raft_pb2.NewNodeBoardcastRequest(
                node_port = request.secondary_port
            )
            with grpc.insecure_channel("localhost:" + str(server_port)) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                try:
                    stub.NewNodeBoardcast(requestb)
                except grpc.RpcError as e:
                    self.all_addresses.remove(server_port)
                    print(f"Leader Server {self.server_id} HeartBeat error to localhost::{server_port}: {e}")
        self.all_addresses.append(request.secondary_port)
        return response

    def NewNodeBoardcast(self,request,context):
        self.next_node_id += 1
        self.all_addresses.append(request.node_port)
        return Empty()

    def DetectHeartBeats(self,request,context):
        print(f"Follower Server {self.server_id} receive HeartBeat")
        self.last_heartbeat_time = time.time()
        self.leader_id = request.leader_id
        if self.role == ROLE_CANDIDATE:
            self.role = ROLE_FOLLOWER
        return raft_pb2.HeartBeatResponse()
    
    def AppendEntries(self, request, context):
        #Handle incoming AppendEntries RPC
        print(f"As Follower, I got {request.term} {request.prev_log_index} {request.prev_log_term}")
        prev_log_index = request.prev_log_index
        if prev_log_index >= len(self.log):
           return raft_pb2.AppendEntriesResponse(term=self.term, success=False, need_log = True)
        if prev_log_index >= 0 and self.log[prev_log_index] != request.prev_log_term:
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False, need_log = False)
        log_index = prev_log_index + 1
        entry = request.entry
        print("Received from leader: "+ request.entry.command)
        # As a follower, current log is ahead of leader, overwrite it
        if log_index < len(self.log):
            self.log = self.log[:log_index]
            self.log.append(entry.term)
            with open(self.log_file, "r") as f:
                lines = f.readlines()
            with open(self.log_file, "w") as f:
                f.writelines(lines[:log_index])

        else:
            self.log.append(request.term)
        with open(self.log_file, "a") as file:
            file.write(str(request.term) + ":" + request.entry.command + "\n")
        return raft_pb2.AppendEntriesResponse(term=self.term, success=True, need_log = False)

    def send_client_responses(self):

        None
    def CheckLeader(self, request, context):
        if self.role == ROLE_LEADER:
            return raft_pb2.CheckLeaderResponse(isLeader = True,leaderPort = self.port)
        else:
            return raft_pb2.CheckLeaderResponse(isLeader = False)
        
    def SendMessage(self, request, context):
        if self.role != ROLE_LEADER:
            return raft_pb2.SendMessageResponse(isSuccessful= False,isLeader = False)
        commitState = 0
        with open(self.log_file, "a") as file:
            file.write(str(self.term) + ":" + request.message + "\n")
        print("Received from client: "+ request.message)
        for server in self.all_addresses:      
            prev_log_index = len(self.log) - 1
            prev_log_term = self.log[prev_log_index] if prev_log_index >= 0 else 0
            current_entry = raft_pb2.LogEntry(term = self.term, command = request.message)
            requestb = raft_pb2.AppendEntriesRequest(
                term=self.term,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entry= current_entry
            )
            commitState += self.send_append_entries(server,requestb)
        if commitState >= len(self.all_addresses) // 2:
            self.log.append((self.term))
            return raft_pb2.SendMessageResponse(isSuccessful= True,isLeader = True)
        else:
            return raft_pb2.SendMessageResponse(isSuccessful= False,isLeader = True)
        
if __name__ == "__main__":
    serverPorts = []
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", type=int, required = True)
    parser.add_argument("-f", type=int)
    args = parser.parse_args()
    server = Server(args.l, args.f)
    server.run()