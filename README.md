# distributed-systems-project

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./raft.proto
```

Client:

1. Client does not recive heartbeat from Primary server.
2. Client will loop through the port range to find the primary server.

## Server Struct:
- self.role # Follower/Candidate/Leader
- self.port # port number of itself
- self.leader # port number of it's leader
- self.need_replay # True if newly added, need to replay the log. False if available to new request
- self.server_id # Unique server id, for logging purpose
self.all_addresses # list of port, excluding leader and itself. Used for Boardcast HeartBeat / Election
- self.log_file # file name of it's disk log file
- self.next_node_id # Next available id for newly added node, only leader can assign node_id to follower
- self.term # term refers to election term, only increase if an election is triggered
- self.commit_index # the commit sequence number
