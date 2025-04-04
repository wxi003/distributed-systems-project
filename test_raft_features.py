import subprocess
import time
import os
import grpc
import pytest
from raft_pb2 import CheckLeaderRequest
from raft_pb2_grpc import RaftServiceStub
import raft_pb2_grpc
import raft_pb2

LOGS = ["LOGNODE0.txt", "LOGNODE1.txt", "LOGNODE2.txt"]
PYTHON_EXEC = "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12"  # for macOS

def cleanup_logs():
    """Remove previous log files if they exist."""
    for log in LOGS:
        try:
            os.remove(log)
        except FileNotFoundError:
            continue

def send_message_to_leader(leader_port, message):
    """Send a client message to the current leader via gRPC."""
    with grpc.insecure_channel(f"localhost:{leader_port}") as channel:
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        request = raft_pb2.SendMessageRequest(message=message)
        response = stub.SendMessage(request)
        return response.isSuccessful

def log_diff_check(files):
    """Check if logs in all files are consistent."""
    logs = []
    for path in files:
        try:
            with open(path, 'r') as f:
                logs.append(f.read().strip())
        except FileNotFoundError:
            logs.append("")
    return all(log == logs[0] for log in logs)

def start_server(port, leader_port=None):
    """Start a RAFT server instance on a given port."""
    if leader_port:
        return subprocess.Popen([PYTHON_EXEC, "nodeRAFT.py", "-l", str(leader_port), "-f", str(port)])
    else:
        return subprocess.Popen([PYTHON_EXEC, "nodeRAFT.py", "-l", str(port)])

def wait_for_leader(ports, retries=10, delay=1):
    """Wait for a leader to be elected among the given ports."""
    for _ in range(retries):
        for port in ports:
            try:
                with grpc.insecure_channel(f"localhost:{port}") as channel:
                    stub = RaftServiceStub(channel)
                    resp = stub.CheckLeader(CheckLeaderRequest(clientID="test"))
                    if resp.isLeader:
                        return port
            except:
                continue
        time.sleep(delay)
    return None

@pytest.fixture(scope="module")
def cluster():
    """Set up a leader and two followers for RAFT testing."""
    cleanup_logs()
    leader = start_server(50051)
    f1 = start_server(50052, 50051)
    f2 = start_server(50053, 50051)
    time.sleep(5)
    yield {
        "leader": leader,
        "followers": [f1, f2],
        "ports": ["50051", "50052", "50053"]
    }
    for p in [leader, f1, f2]:
        p.terminate()
    time.sleep(1)
    cleanup_logs()

def test_start_cluster(cluster):
    """Test that a leader is elected when the cluster starts."""
    leader_port = wait_for_leader(cluster["ports"])
    assert leader_port is not None, "No leader elected"

def test_send_message_success(cluster):
    """Test that a message is successfully replicated to followers."""
    leader_port = wait_for_leader(cluster["ports"])
    assert send_message_to_leader(leader_port, "log entry success")

def test_leader_election(cluster):
    """Test that a new leader is elected after the current one crashes."""
    cluster["leader"].terminate()
    time.sleep(25)
    new_leader = wait_for_leader(["50052", "50053"], retries=15)
    assert new_leader is not None, "No new leader elected after crash"
    cluster["leader_port"] = new_leader

