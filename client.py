import grpc
import replication_pb2
import replication_pb2_grpc
import threading

file_lock = threading.Lock()

file_open = open('client.txt', 'w')
file_open.close()

def send_request(stub, request_data):
    request = replication_pb2.WriteRequest(key=request_data['key'], value=request_data['value'])
    
    # Log the request to client.txt before receiving the acknowledgment
    with file_lock:
        with open('client.txt', 'a') as file:
            file.write(f"Client sent: {request.key} {request.value}\n")
    
    # Send the request to the primary
    response = stub.Write(request)

def create_threads():
    # Define the list of requests you want to send in parallel
    requests = [
        {"key": "1", "value": "book"},
        {"key": "2", "value": "pen"},
        {"key": "3", "value": "laptop"},
        # Add more requests as needed
    ]
    
    # Establish the connection and create the stub once
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = replication_pb2_grpc.SequenceStub(channel)
        
        threads = []
        
        # Create a thread for each request
        for req in requests:
            thread = threading.Thread(target=send_request, args=(stub, req))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    create_threads()