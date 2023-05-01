import datetime
import logging
import socket
import time
from threading import Thread
from typing import Dict, List, Set, Tuple
from raft_node import RaftNode

from utils import DEFAULT_HOST, DEFAULT_PORT, Message, MessageTypes, NodeState


class Broker(RaftNode):
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        backlog: int = 5,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = None,
    ):
        RaftNode.__init__(self, host, port, peers, election_timeout)
        self.logger = logging.getLogger(__name__ + f" {self.port}")
        self.backlog = backlog

        # Store subscribed clients for each topic
        #  e.g. { 'topic': set(socket) }
        self.topic_sockets: Dict[str, Set[socket.socket]] = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # TODO: refactor topic_socket to be topic_host_port, so that the info can be transferred to another broker node.
    # TODO: This means `subscriber` need to spin up like a http server
    def handle_client(self, client_socket: socket.socket, address) -> None:
        """Handle client connections, for both SUBSCRIBE and PUBLISH requests

        Args:
            client_socket (socket.socket): client socket
            address (_type_): client socket address
        """
        while True:
            try:
                data = client_socket.recv(1024)
            except OSError:
                break
            if not data:
                break
            msg = Message.from_bytes(data)

            if msg.type == MessageTypes.PUBLISH:
                # Handle PUBLISH message
                self.logger.info(f"Publish topic {msg.topic}: {msg.content} from {address}")
                if msg.topic in self.topic_sockets:
                    # Forward message to all clients subscribed to this topic
                    for subscribe_socket in self.topic_sockets[msg.topic]:
                        subscribe_socket.sendall(msg.to_bytes())
            elif msg.type == MessageTypes.SUBSCRIBE:
                # Handle SUBSCRIBE message
                if not self.is_leader:
                    self.forward_to_leader(client_socket, msg)
                else:
                    self.logger.info(f"New subscription to topic {msg.topic} from {address}")
                    if msg.topic not in self.topic_sockets:
                        # Create a new set to store subscribed clients for this topic
                        self.topic_sockets[msg.topic] = set()
                    self.topic_sockets[msg.topic].add(client_socket)
            elif msg.type == MessageTypes.HEARTBEAT:
                self.on_receive_heartbeat(client_socket, msg, datetime.datetime.now())
            elif msg.type == MessageTypes.REQUEST_TO_VOTE:
                self.vote(client_socket, msg)

        # Close the client socket when the connection is terminated
        self.logger.debug(f"Disconnect from {address}")
        client_socket.close()
        # Remove the client socket in each topic
        for socket_set in self.topic_sockets.values():
            socket_set.discard(client_socket)

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        self.logger.info(f"Running on {self.host}:{self.port}")
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)

        # Start heartbeat thread
        Thread(target=self.send_heartbeats).start()

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
            # when `self.stop()` is invoked, it closes server_socket and yields this exception
            except ConnectionAbortedError:
                break

    def stop(self):
        """Stop the broker properly to avoid error `OSError: [Errno 48] Address already in use`"""
        self.set_stopped(True)
        self.server_socket.close()
        for socket_set in self.topic_sockets.values():
            socket_list = list(socket_set)
            while len(socket_list) > 0:
                socket_list.pop(0).close()


if __name__ == "__main__":
    # log settings
    logger = logging.getLogger(Broker.__name__)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    host_ips = [(DEFAULT_HOST, DEFAULT_PORT), (DEFAULT_HOST, DEFAULT_PORT + 1)]

    broker1 = Broker(host=host_ips[0][0], port=host_ips[0][1], peers=[host_ips[1]])
    broker1.run()
    broker2 = Broker(host=host_ips[1][0], port=host_ips[1][1], peers=[host_ips[0]])
    broker2.run()
