import grpc
import station_pb2
import station_pb2_grpc
import sys

SERVER = "localhost:5440"

def run():
    if len(sys.argv) != 2:
        print("Usage: python3 ClientStationMax.py <StationID>")
        sys.exit(1)
    stationID = sys.argv[1]

    # Connect to the gRPC server
    with grpc.insecure_channel('localhost:5440') as channel:
        stub = station_pb2_grpc.StationStub(channel)
        # Send the request and get the number of stations
        response = stub.StationName(station_pb2.StationInspectRequest(station = stationID))
        if response.error != "":
            print(response.error)
        else:
            print(response.name)

if __name__ == '__main__':
    run()