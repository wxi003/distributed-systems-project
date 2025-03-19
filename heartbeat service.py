import grpc
import heartbeat_service_pb2_grpc
import time
import threading
from concurrent import futures
from datetime import datetime
# from google.protobuf import empty_pb2
from google.protobuf.empty_pb2 import Empty

primaryLastTime = datetime.min

backupLastTime = datetime.min

file_lock = threading.Lock()

file_open = open('heartbeat.txt', 'w')
file_open.close()

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):
    def Heartbeat(self, request, context):
        global primaryLastTime
        global backupLastTime
        currTime = datetime.now()
        if(request.service_identifier == "primary"):
            primaryLastTime = currTime
            with file_lock:
                with open('heartbeat.txt', 'a') as file_open:
                    file_open.write(f"Primary is alive. Latest heartbeat received at {currTime}\n")
        elif(request.service_identifier == "backup"):
            backupLastTime = currTime
            with file_lock:
                with open('heartbeat.txt', 'a') as file_open:
                    file_open.write(f"Backup is alive. Latest heartbeat received at {currTime}\n")
        return Empty()
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(ViewServiceServicer(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    print("Primary server running on port 50053")
    server.wait_for_termination()

def check():
    global primaryLastTime
    global backupLastTime
    while(True):
        currTime = datetime.now()
        if(primaryLastTime < currTime and primaryLastTime != datetime.min):
            with file_lock:
                with open('heartbeat.txt', 'a') as file_open:
                    file_open.write(f"Primary might be down. Latest heartbeat received at {primaryLastTime}\n")
        if(backupLastTime < currTime and backupLastTime != datetime.min):
            with file_lock:
                with open('heartbeat.txt', 'a') as file_open:
                    file_open.write(f"Backup might be down. Latest heartbeat received at {backupLastTime}\n")
        time.sleep(15)

if __name__ == '__main__':
    t1 = threading.Thread(target=serve, args=())
    t2 = threading.Thread(target=check, args=())
    t1.start()
    t2.start()
    t1.join()
    t2.join()