import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2_grpc
import heartbeat_service_pb2
from concurrent import futures
import time
import threading

# Store the data in a Python dictionary
data_store = {}

file_lock = threading.Lock()

stub = replication_pb2_grpc.SequenceStub(grpc.insecure_channel('localhost:50052'))

heartStub = heartbeat_service_pb2_grpc.ViewServiceStub(grpc.insecure_channel('localhost:50053'))

file_open = open('primary.txt', 'w')
file_open.close()

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def Write(self, request, context):
        # Log the request to primary.txt
        with file_lock:
            with open('primary.txt', 'a') as file:
                file.write(f"{request.key} {request.value}\n")
            
            # Replicate the data to the backup server
            response = stub.Write(request)
            
            # Apply the write to the primary data store
            data_store[request.key] = request.value
            
            # Log the operation to the primary file again
            with open('primary.txt', 'a') as file:
                file.write(f"Applied to primary: {request.key} {request.value}\n")
            
        # Return the acknowledgment to the client
        return replication_pb2.WriteResponse(ack="Write successful to primary and backup.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(SequenceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Primary server running on port 50051")
    server.wait_for_termination()

def sendHeartBeat():
    while(True):
        try:
            heartStub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier="primary"))
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()}, {e.details()}")
        time.sleep(5)

if __name__ == '__main__':
    t1 = threading.Thread(target=serve, args=())
    t2 = threading.Thread(target=sendHeartBeat, args=())
    t1.start()
    t2.start()
    t1.join()
    t2.join()
