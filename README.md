# distributed-systems-project

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./raft.proto

Client:

1. Client does not recive heartbeat from Primary server.
2. Client will loop through the port range to find the primary server.
