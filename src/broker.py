import logging
import socket
import time
from threading import Thread
from typing import Dict, List, Set, Tuple

from utils import DEFAULT_HOST, DEFAULT_PORT, Message, MessageTypes, NodeState

logger = logging.getLogger(__name__)


class Broker:
    HEARTBEAT_INTERVAL = 5  # seconds
    HEARTBEAT_TIMEOUT = 10  # seconds

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        backlog: int = 5,
        peers: List[Tuple[str, int]] = None,
    ):
        self.host = host
        self.port = port
        self.backlog = backlog
        self.state = NodeState.FOLLOWER

        # Store subscribed clients for each topic
        #  e.g. { 'topic': set(socket) }
        self.topic_sockets: Dict[str, Set[socket.socket]] = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # peer brokers
        self.peers = peers or []
        self.peer_sockets: List[socket.socket] = []
        self.last_heartbeat_sent = None
        self.stopped = False

    def handle_client(self, client_socket: socket.socket, address) -> None:
        """Handle client connections, for both SUBSCRIBE and PUBLISH requests

        Args:
            client_socket (socket.socket): client socket
            address (_type_): client socket address
        """
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            msg = Message.from_bytes(data)

            if msg.type == MessageTypes.PUBLISH:
                # Handle PUBLISH message
                logger.info(f"Publish topic {msg.topic}: {msg.content} from {address}")
                if msg.topic in self.topic_sockets:
                    # Forward message to all clients subscribed to this topic
                    for subscribe_socket in self.topic_sockets[msg.topic]:
                        subscribe_socket.sendall(msg.to_bytes())
            elif msg.type == MessageTypes.SUBSCRIBE:
                # Handle SUBSCRIBE message
                logger.info(f"New subscription to topic {msg.topic} from {address}")
                if msg.topic not in self.topic_sockets:
                    # Create a new set to store subscribed clients for this topic
                    self.topic_sockets[msg.topic] = set()
                self.topic_sockets[msg.topic].add(client_socket)
            elif msg.type == MessageTypes.HEARTBEAT:
                # Handle HEARTBEAT message
                msg = Message(MessageTypes.ACK)
                client_socket.sendall(msg.to_bytes())

        # Close the client socket when the connection is terminated
        logger.debug(f"Disconnect from {address}")
        client_socket.close()
        # Remove the client socket in each topic
        for socket_set in self.topic_sockets.values():
            socket_set.discard(client_socket)

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        logger.info(f"RUNNING on {self.host}:{self.port}")
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)

        # Start heartbeat thread
        Thread(target=self.send_heartbeats).start()

        # Start accepting client connections
        Thread(target=self.accept_client_connections).start()

    def send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        while not self.stopped:
            msg = Message(MessageTypes.HEARTBEAT)
            for peer_host, peer_port in self.peers:
                Thread(
                    target=self.__deal_with_one_heartbeat,
                    args=(peer_host, peer_port, msg),
                ).start()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def __deal_with_one_heartbeat(self, peer_host: str, peer_port: int, msg: Message):
        """Send heartbeat to one peer

        Args:
            peer_host (str): peer host
            peer_port (int): peer port
            msg (Message): message to send (HEARTBEAT)
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(self.HEARTBEAT_TIMEOUT)
                s.connect((peer_host, peer_port))
                logger.debug(f"Connected to peer {peer_host}:{peer_port}")
                s.sendall(msg.to_bytes())
                data = s.recv(1024)
                if not data:
                    return
                msg = Message.from_bytes(data)
                logger.debug(f"Received ACK from {peer_host}:{peer_port}")
                # todo: set last_ack_time for this peer
                self.last_heartbeat_sent = time.time()
        except ConnectionRefusedError:
            logger.error(f"Failed to connect to peer {peer_host}:{peer_port}")
        except socket.timeout:
            # todo: peer fails
            pass

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
        self.stopped = True
        self.server_socket.close()
        for socket_set in self.topic_sockets.values():
            for client_socket in socket_set:
                client_socket.close()


if __name__ == "__main__":
    # log settings
    logger = logging.getLogger(Broker.__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    host_ips = [(DEFAULT_HOST, DEFAULT_PORT), (DEFAULT_HOST, DEFAULT_PORT + 1)]

    broker1 = Broker(host=host_ips[0][0], port=host_ips[0][1], peers=[host_ips[1]])
    broker1.run()
    broker2 = Broker(host=host_ips[1][0], port=host_ips[1][1], peers=[host_ips[0]])
    broker2.run()
