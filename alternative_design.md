# Alternative Designs for KegDisplayDB Sync Service

## Current Design Issues

The current sync service uses Flask as an HTTP-based RPC wrapper around the database service. This adds overhead:

- HTTP request/response cycle overhead
- Flask routing and middleware processing
- JSON serialization/deserialization
- Multiple layers of abstractions

## Alternative 1: Direct Socket-based RPC with Protocol Buffers

### Benefits:
- Much faster than HTTP
- Strongly typed interfaces
- Binary protocol reduces bandwidth
- Lower CPU and memory overhead

### Implementation:
```python
# server.py
import socket
import threading
import beer_pb2  # Generated Protocol Buffer code

class SyncServer:
    def __init__(self, db, port=5001):
        self.db = db
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('0.0.0.0', port))
        self.running = False
        
    def start(self):
        self.running = True
        self.sock.listen(5)
        threading.Thread(target=self._accept_connections, daemon=True).start()
        
    def _accept_connections(self):
        while self.running:
            client, addr = self.sock.accept()
            threading.Thread(target=self._handle_client, args=(client, addr), daemon=True).start()
            
    def _handle_client(self, client, addr):
        try:
            while self.running:
                # Read message type and size
                header = client.recv(8)
                if not header:
                    break
                
                msg_type, msg_size = struct.unpack("!II", header)
                data = client.recv(msg_size)
                
                # Process based on message type
                if msg_type == 1:  # Get all beers
                    response = beer_pb2.BeerList()
                    for beer in self.db.get_all_beers():
                        beer_pb = response.beers.add()
                        # Fill beer data
                    client.sendall(response.SerializeToString())
                elif msg_type == 2:  # Add beer
                    # etc.
        finally:
            client.close()
```

### Client Code:
```python
# client.py
import socket
import struct
import beer_pb2

class SyncClient:
    def __init__(self, host="localhost", port=5001):
        self.host = host
        self.port = port
        
    def get_beers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            
            # Send request type 1 (get all beers)
            sock.sendall(struct.pack("!II", 1, 0))  # Type 1, empty payload
            
            # Get response
            data = sock.recv(4096)
            beer_list = beer_pb2.BeerList()
            beer_list.ParseFromString(data)
            
            return [self._pb_to_dict(beer) for beer in beer_list.beers]
            
    def _pb_to_dict(self, beer_pb):
        return {
            "id": beer_pb.id,
            "name": beer_pb.name,
            # etc.
        }
```

## Alternative 2: gRPC Service

### Benefits:
- Lightweight RPC framework built on HTTP/2
- Strongly typed interfaces with Protocol Buffers
- Built-in service discovery and authentication
- Auto-generated client and server code

### Implementation:
```proto
// beer.proto
syntax = "proto3";

service BeerService {
  rpc GetAllBeers(Empty) returns (BeerList);
  rpc GetBeer(BeerId) returns (Beer);
  rpc AddBeer(Beer) returns (BeerId);
  rpc UpdateBeer(Beer) returns (Status);
  rpc DeleteBeer(BeerId) returns (Status);
  // etc.
}

message Empty {}

message BeerId {
  int32 id = 1;
}

message Beer {
  int32 id = 1;
  string name = 2;
  float abv = 3;
  int32 ibu = 4;
  string color = 5;
  // etc.
}

message BeerList {
  repeated Beer beers = 1;
}

message Status {
  bool success = 1;
  string message = 2;
}
```

### Server Code:
```python
# server.py
import grpc
import beer_pb2
import beer_pb2_grpc
from concurrent import futures

class BeerServicer(beer_pb2_grpc.BeerServiceServicer):
    def __init__(self, db):
        self.db = db
        
    def GetAllBeers(self, request, context):
        response = beer_pb2.BeerList()
        for beer in self.db.get_all_beers():
            beer_pb = response.beers.add()
            beer_pb.id = beer["idBeer"]
            beer_pb.name = beer["Name"]
            # etc.
        return response
        
    # Other methods...

def serve(db):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    beer_pb2_grpc.add_BeerServiceServicer_to_server(BeerServicer(db), server)
    server.add_insecure_port('[::]:5001')
    server.start()
    server.wait_for_termination()
```

### Client Code:
```python
# client.py
import grpc
import beer_pb2
import beer_pb2_grpc

class BeerClient:
    def __init__(self, host="localhost", port=5001):
        channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = beer_pb2_grpc.BeerServiceStub(channel)
    
    def get_beers(self):
        response = self.stub.GetAllBeers(beer_pb2.Empty())
        return [self._pb_to_dict(beer) for beer in response.beers]
        
    # Other methods...
```

## Alternative 3: ZeroMQ for Lightweight Messaging

### Benefits:
- Extremely lightweight and fast
- Flexible messaging patterns (req/rep, pub/sub, push/pull)
- No central broker needed
- Built for high-throughput applications

### Implementation:
```python
# server.py
import zmq
import json

class BeerServer:
    def __init__(self, db, port=5001):
        self.db = db
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")
        
    def start(self):
        while True:
            msg = self.socket.recv_json()
            
            action = msg.get("action")
            if action == "get_beers":
                self.socket.send_json({"beers": self.db.get_all_beers()})
            elif action == "add_beer":
                beer_id = self.db.add_beer(**msg.get("data", {}))
                self.socket.send_json({"beer_id": beer_id})
            # etc.
```

### Client Code:
```python
# client.py
import zmq
import json

class BeerClient:
    def __init__(self, host="localhost", port=5001):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{host}:{port}")
        
    def get_beers(self):
        self.socket.send_json({"action": "get_beers"})
        return self.socket.recv_json().get("beers", [])
        
    def add_beer(self, beer_data):
        self.socket.send_json({"action": "add_beer", "data": beer_data})
        return self.socket.recv_json().get("beer_id")
```

## Alternative 4: Direct Integration (No Separate Service)

### Benefits:
- Eliminates network overhead entirely
- Simplifies architecture
- Reduces deployment complexity

### Implementation:
Instead of running a separate sync service, embed the database functionality directly:

```python
# app.py
from KegDisplayDB.db import SyncedDatabase

# Create and use database directly
db = SyncedDatabase('path/to/db.sqlite')

# Use in web app
@app.route('/api/beers')
def get_beers():
    return jsonify(db.get_all_beers())
```

## Recommendation

Based on the application's needs, gRPC (Alternative 2) offers the best balance of:
- Performance improvement over HTTP/Flask
- Strong typing for better safety
- Modern tooling and ecosystem
- Maintainability
- Scalability for future growth

For an even more lightweight option, ZeroMQ provides excellent performance with minimal dependencies. 