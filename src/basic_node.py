import threading
import time

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.is_leader = False

    def send_election_message(self, node):
        print(f"Node {self.node_id} sends an election message to Node {node.node_id}")

    def send_coordinator_message(self, node):
        print(f"Node {self.node_id} sends a coordinator message to Node {node.node_id}")

    def start_election(self):
        print(f"Node {self.node_id} starts the election process")
        for node_id in range(self.node_id + 1, 10):
            node = Node(node_id)
            self.send_election_message(node)

    def become_coordinator(self):
        self.is_leader = True
        print(f"Node {self.node_id} becomes the coordinator")

    def handle_election_message(self, node):
        print(f"Node {self.node_id} received an election message from Node {node.node_id}")
        if self.node_id > node.node_id:
            self.send_coordinator_message(node)
        elif self.node_id < node.node_id:
            # Node is lower in ID, so it stops the election process
            print(f"Node {self.node_id} stops the election process")

    def handle_coordinator_message(self, node):
        print(f"Node {self.node_id} received a coordinator message from Node {node.node_id}")
        if not self.is_leader:
            print(f"Node {self.node_id} acknowledges Node {node.node_id} as the leader")
        else:
            print(f"Node {self.node_id} is already the leader")

def simulate_election():
    nodes = [Node(i) for i in range(1, 11)]
    coordinator = None

    # Simulate an initial coordinator failure
    nodes[0].become_coordinator()
    coordinator = nodes[0]

    # Start the election process
    time.sleep(3)
    nodes[0].start_election()

    # Wait for some time to simulate message delays
    time.sleep(2)

    # Handle election messages
    for node in nodes[1:]:
        coordinator = node
        node.handle_election_message(coordinator)
        if node.is_leader:
            break

    # Simulate some additional messages
    time.sleep(1)

    # Handle coordinator messages
    for node in nodes:
        node.handle_coordinator_message(coordinator)

simulate_election()
