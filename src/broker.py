import logging
import socket
from threading import Thread
from typing import Dict, List, Set, Tuple
from raft_node import RaftNode

from utils import BROKER_HOST, BROKER_PORT, Message, MessageTypes


class Broker(RaftNode):
    def __init__(
        self,
        host: str = BROKER_HOST,
        port: int = BROKER_PORT,
        backlog: int = 5,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = 0,
    ):
        RaftNode.__init__(self, host, port, peers, election_timeout)
        self.backlog = backlog

        # Store subscribed clients for each topic
        #  e.g. { 'topic': set((host1, port1), (host2, port2), ...) }
        self.topic_subscribers: Dict[str, Set[Tuple[str, int]]] = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def handle_subscribe(self, msg: Message):
        if msg.topic not in self.topic_subscribers:
            # Create a new set to store subscribed clients for this topic
            self.topic_subscribers[msg.topic] = set()
        host_port = (msg.dest_host, int(msg.dest_port))
        self.topic_subscribers[msg.topic].add(host_port)

    def handle_unsubscribe(self, msg: Message):
        if msg.topic in self.topic_subscribers:
            host_port = (msg.dest_host, int(msg.dest_port))
            self.topic_subscribers[msg.topic].discard(host_port)

    def handle_append_entries(self, append_entries: Message):
        """Handle an append_entries message from the leader"""
        self.logger.info("Handle append_entries from the leader")
        if append_entries.type == MessageTypes.SUBSCRIBE:
            self.handle_subscribe(append_entries)
        elif append_entries.type == MessageTypes.UNSUBSCRIBE:
            self.handle_unsubscribe(append_entries)

    def handle_client(self, client_socket: socket.socket, address) -> None:
        """Handle client connections, include UN/SUBSCRIBE, PUBLISH,
        and cluster message like HEARTBEAT, REQUEST_TO_VOTE

        Args:
            client_socket (socket.socket): client socket
            address (_type_): client socket address
        """
        try:
            data = client_socket.recv(1024)
        except OSError:
            return
        if not data:
            return
        msg = Message.from_bytes(data)

        if msg.type == MessageTypes.PUBLISH:
            # Handle PUBLISH message
            self.logger.info(f"New publish `{msg.topic}`: `{msg.content}`")
            if msg.topic in self.topic_subscribers:
                # Forward message to all subscribers in the same topic
                for host, port in self.topic_subscribers[msg.topic]:
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((host, port))
                            s.sendall(msg.to_bytes())
                    except ConnectionRefusedError:
                        pass
        elif msg.type == MessageTypes.SUBSCRIBE or msg.type == MessageTypes.UNSUBSCRIBE:
            # Handle UN/SUBSCRIBE message
            if not self.is_leader:
                # Forward to leader if current node is not a leader
                self.forward_to_leader(client_socket, msg)
            else:
                self.logger.info(
                    f"New {msg.type.name} to `{msg.topic}` from {msg.dest_host}:{msg.dest_port}"
                )
                if msg.type == MessageTypes.SUBSCRIBE:
                    self.setup_append_entries(msg, self.handle_subscribe)
                elif msg.type == MessageTypes.UNSUBSCRIBE:
                    self.setup_append_entries(msg, self.handle_unsubscribe)
        elif msg.type == MessageTypes.HEARTBEAT:
            self.on_receive_heartbeat(client_socket, msg)
        elif msg.type == MessageTypes.REQUEST_TO_VOTE:
            self.handle_request_to_vote(client_socket, msg)

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        self.logger.info(f"Running on {self.host}:{self.port}")
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)

        # RaftNode run (leader election)
        super().run()

        # Start accepting client connections
        Thread(target=self.accept_client_connections).start()

    def accept_client_connections(self) -> None:
        """Accept connections from clients (Publisher / Subscriber)"""
        while not self.stopped:
            try:
                # Accept new client connections and start a new thread to handle each client
                client_socket, address = self.server_socket.accept()
                Thread(
                    target=self.handle_client,
                    args=(client_socket, address),
                    daemon=True,
                ).start()
            except ConnectionAbortedError:
                # when `self.stop()` is invoked, it closes server_socket and yields this exception
                break

    def stop(self):
        """Stop the broker properly to avoid error `OSError: [Errno 48] Address already in use`"""
        self.server_socket.close()
        self.set_stopped(True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1)]

    broker1 = Broker(host=host_ips[0][0], port=host_ips[0][1], peers=[host_ips[1]])
    broker1.run()
    broker2 = Broker(host=host_ips[1][0], port=host_ips[1][1], peers=[host_ips[0]])
    broker2.run()
