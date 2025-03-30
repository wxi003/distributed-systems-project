import threading
import time
import random
import grpc
import raft_pb2
import raft_pb2_grpc
from concurrent import futures

#Define roles as constants
ROLE_FOLLOWER = "Follower"
ROLE_CANDIDATE = "Candidate"
ROLE_LEADER = "Leader"
commitState = False
class Server(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, server_id, all_addresses):
        #Initialize a server with RAFT state
        self.server_id = server_id
        self.all_addresses = all_addresses  #['localhost:50001', ...]
        self.port = int(self.all_addresses[self.server_id].split(':')[-1])
        self.term = 0
        self.role = ROLE_FOLLOWER
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(10, 17)
        self.votes_received = 0
        self.next_index = None
        self.match_index = None
        self.last_heartbeat_sent = 0
        self.heartbeat_interval = 5
        self.leader_id = None
        self.pending_requests = []  #For client requests (not sure if needed)
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
            with self.lock:
                if self.role in [ROLE_FOLLOWER, ROLE_CANDIDATE]:
                    if current_time - self.last_heartbeat_time > self.election_timeout:
                        print(f"Server {self.server_id} started an election.")
                        self.start_election()
                if self.role == ROLE_LEADER:
                    if current_time - self.last_heartbeat_sent > self.heartbeat_interval:
                        self.send_heartbeats()
            time.sleep(0.1)  # Avoid busy waiting

    def start_election(self):
        #Start a leader election as a Candidate          
        self.role = ROLE_CANDIDATE
        self.term += 1
        self.voted_for = self.server_id
        self.votes_received = 1  # Vote for self
        self.last_heartbeat_time = time.time()
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1][0] if self.log else 0
        request = raft_pb2.RequestVoteRequest(
            term=self.term,
            candidate_id=self.server_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
                
        #Send RequestVote RPC to all other servers concurrently
        def send_request_vote(to_server_id):
            address = self.all_addresses[to_server_id]
            print(f"Server {self.server_id} sending request vote to {to_server_id}")
            with grpc.insecure_channel(address) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                try:
                    response = stub.RequestVote(request)
                    if response.term > self.term:
                        self.term = response.term
                        self.voted_for = None
                        self.role = ROLE_FOLLOWER
                        return
                    if response.vote_granted and self.role == ROLE_CANDIDATE:
                        self.votes_received += 1
                        if self.votes_received > len(self.all_addresses) // 2:
                            self.role = ROLE_LEADER
                            self.leader_id = self.server_id
                            print(f"Server {self.server_id} become leader")
                            self.send_heartbeats()
                except grpc.RpcError as e:
                    print(f"Server {self.server_id} RPC error to {to_server_id}: {e}")
        with futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(send_request_vote, [s for s in range(len(self.all_addresses)) if s != self.server_id])

    def send_heartbeats(self):
        #Leader sends AppendEntries RPCs as heartbeats
        if self.next_index is None:
            self.next_index = {s: len(self.log) for s in range(len(self.all_addresses)) if s != self.server_id}
            self.match_index = {s: 0 for s in range(len(self.all_addresses)) if s != self.server_id}
        for server in range(len(self.all_addresses)):
            if server != self.server_id:
                # next_idx = self.next_index[server]
                # prev_log_index = next_idx - 1
                # prev_log_term = self.log[prev_log_index][0] if prev_log_index >= 0 else 0
                # entries = [] #[raft_pb2.LogEntry(term=t, command=c) for t, c in self.log[next_idx:]]
                request = raft_pb2.HeartBeat(
                    # term=self.term,
                    leader_id=self.server_id
                    # prev_log_index=prev_log_index,
                    # prev_log_term=prev_log_term,
                    # entries=entries,
                    # leader_commit=self.commit_index
                )

                
                # Send AppendEntries in a separate thread
                threading.Thread(target=self.send_heart_beat, args=(server,request)).start()
        self.last_heartbeat_sent = time.time()

    def send_heart_beat(self,to_server_id,request):
        address = self.all_addresses[to_server_id]
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.DetectHeartBeats(request)
            except grpc.RpcError as e:
                print(f"Server {self.server_id} RPC error to {to_server_id}: {e}")
    def send_append_entries(self, to_server_id, request):
        global commitState
        address = self.all_addresses[to_server_id]
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.AppendEntries(request)
                
                if response.term > self.term:
                    self.term = response.term
                    self.role = ROLE_FOLLOWER
                    self.voted_for = None
                    return
              
                if response.success:
                    self.match_index[to_server_id] = request.prev_log_index + len(request.entries)
                    self.next_index[to_server_id] = self.match_index[to_server_id] + 1
                    # Update commit_index based on majority replication
                    for n in range(self.commit_index, len(self.log)):
                        if sum(1 for s in self.match_index if self.match_index[s] >= n) > len(self.all_addresses) // 2:
                            self.commit_index = n
                            self.send_client_responses()                            
                            commitState=True
                            break
                            
                else:
                    print("not good")
                    self.next_index[to_server_id] = max(1, self.next_index[to_server_id] - 1)
            except grpc.RpcError as e:
                print(f"Server {self.server_id} RPC error to {to_server_id}: {e}")

    def RequestVote(self, request, context):
        #Handle incoming RequestVote RPC
        if request.term > self.term:
            self.term = request.term
            self.voted_for = None
            self.role = ROLE_FOLLOWER
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1][0] if self.log else 0
        log_ok = (request.last_log_term > last_log_term) or \
                    (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
        if request.term >= self.term and (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok:
            self.voted_for = request.candidate_id
            self.last_heartbeat_time = time.time()
            print(f"Vote: Server {self.server_id} voted server {self.voted_for}")
            return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=True)
        return raft_pb2.RequestVoteResponse(term=self.term, vote_granted=False)
    def DetectHeartBeats(self,request,context):
        self.last_heartbeat_time = time.time()
        self.leader_id = request.leader_id
        if self.role == ROLE_CANDIDATE:
            self.role = ROLE_FOLLOWER
        return raft_pb2.HeartBeatResponse()
    def AppendEntries(self, request, context):
        #Handle incoming AppendEntries RPC
         if request.term < self.term:
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False)
          
        #  self.last_heartbeat_time = time.time()
        #  self.leader_id = request.leader_id
        #  if self.role == ROLE_CANDIDATE:
        #     self.role = ROLE_FOLLOWER
         prev_log_index = request.prev_log_index
         if prev_log_index >= len(self.log):
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False)
         if prev_log_index >= 0 and self.log[prev_log_index][0] != request.prev_log_term:
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False)
        #Apply log entries
         for i, entry in enumerate(request.entries):
            log_index = prev_log_index + 1 + i
            if log_index < len(self.log):
                if self.log[log_index][0] != entry.term:
                    self.log = self.log[:log_index]
                    self.log.append((entry.term, entry.command))
            else:
                self.log.append((entry.term, entry.command))
         if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log) - 1)
         return raft_pb2.AppendEntriesResponse(term=self.term, success=True)

    def send_client_responses(self):

        None
    def CheckLeader(self, request, context):
        if self.server_id == self.leader_id:
            return raft_pb2.CheckLeaderResponse(isLeader = True,leaderPort = self.all_addresses[self.leader_id].split(':')[-1])
        else:
            return raft_pb2.CheckLeaderResponse(isLeader = False,leaderPort = self.all_addresses[self.leader_id].split(':')[-1])
    def SendMessage(self, request, context):
        global commitState
        self.log.append((self.term,request.message))
        print("Received from client: "+request.message)
        for server in range(len(self.all_addresses)):
            if server != self.server_id:
              next_idx = self.next_index[server]           
              prev_log_index = next_idx - 1
              prev_log_term = self.log[prev_log_index][0] if prev_log_index >= 0 else 0
              entries = [raft_pb2.LogEntry(term=t, command=c) for t, c in self.log[next_idx:]]
              request = raft_pb2.AppendEntriesRequest(
                    term=self.term,
                    leader_id=self.server_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
              
              threading.Thread(target=self.send_append_entries, args=(server,request)).start()
        
        time.sleep(0.5)
        if commitState == True:
            if self.server_id == self.leader_id:
             commitState = False
             return raft_pb2.SendMessageResponse(isSuccessful= True,isLeader = True)
            else:
             commitState = False
             return raft_pb2.SendMessageResponse(isSuccessful= True,isLeader = False)
        else:
            if self.server_id == self.leader_id:
             return raft_pb2.SendMessageResponse(isSuccessful= False,isLeader = True)
            else:
             return raft_pb2.SendMessageResponse(isSuccessful= False,isLeader = False)
if __name__ == "__main__":
    serverPorts = []
    while True:
        newPort = input("Please enter server port, enter q to finish: ")
        if newPort != "q":
            serverPorts.append(newPort)
        else:
            break
    serverPorts = list(dict.fromkeys(serverPorts))
    
    allAddresses = [f'localhost:{i}' for i in serverPorts]
    servers = [Server(i, allAddresses) for i in range(len(serverPorts))]
    threads = [threading.Thread(target=server.run) for server in servers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()