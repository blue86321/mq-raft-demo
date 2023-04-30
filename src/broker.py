import logging
import socket
from threading import Thread
from typing import Dict, Set

from utils import DEFAULT_HOST, DEFAULT_PORT, Message, MessageTypes

logger = logging.getLogger(__name__)


class Broker:
    def __init__(
        self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, backlog: int = 5
    ):
        self.host = host
        self.port = port
        self.backlog = backlog

        # Store subscribed clients for each topic
        #  e.g. { 'topic': set(socket) }
        self.topic_sockets: Dict[str, Set[socket.socket]] = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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

        # Close the client socket when the connection is terminated
        logger.debug(f"Disconnect from {address}")
        client_socket.close()
        # Remove the client socket in each topic
        for socket_set in self.topic_sockets.values():
            socket_set.discard(client_socket)

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)
        while True:
            try:
                # Accept new client connections and start a new thread to handle each client
                client_socket, address = self.server_socket.accept()
                thread = Thread(
                    target=self.handle_client, args=(client_socket, address)
                )
                thread.daemon = True
                thread.start()
            # when `self.stop()` is invoked, it closes server_socket and yields this exception
            except ConnectionAbortedError:
                break

    def stop(self):
        """Stop the broker properly to avoid error 'OSError: [Errno 48] Address already in use' """
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

    broker = Broker()
    broker.run()
