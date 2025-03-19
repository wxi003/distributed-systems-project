import grpc
import replication_pb2
import replication_pb2_grpc
from concurrent import futures
import heartbeat_service_pb2_grpc
import heartbeat_service_pb2
import time
import threading

# Store the data in a Python dictionary for the backup
backup_data_store = {}

heartStub = heartbeat_service_pb2_grpc.ViewServiceStub(grpc.insecure_channel('localhost:50053'))

file_open = open('backup.txt', 'w')
file_open.close()

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def Write(self, request, context):
        # Log the request to backup.txt
        with open('backup.txt', 'a') as file:
            file.write(f"{request.key} {request.value}\n")
        
        # Apply the write to the backup data store
        backup_data_store[request.key] = request.value
        
        # Log the operation to the backup file
        with open('backup.txt', 'a') as file:
            file.write(f"Applied to backup: {request.key} {request.value}\n")
        
        # Return the acknowledgment to the primary
        return replication_pb2.WriteResponse(ack="Write successful to backup.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(SequenceServicer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Backup server running on port 50052")
    server.wait_for_termination()

def sendHeartBeat():
    while(True):
        try:
            heartStub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier="backup"))
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
