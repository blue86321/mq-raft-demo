import logging
import socket
from threading import Thread
from typing import Tuple

from src.utils import CLUSTER_HOST, CLUSTER_PORT, Message, MessageTypes


class ClusterManager:
    def __init__(
        self, host: str = CLUSTER_HOST, port: int = CLUSTER_PORT, backlog: int = 5
    ) -> None:
        self.host = host
        self.port = port
        self.backlog = backlog
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stopped = False
        self.logger = logging.getLogger(f"{self.__class__.__name__} {self.port}")

        self.leader_host_port: Tuple[str, int] = ('', 0)
        self.peers: set[Tuple[str, int]] = set()

    def handle_client(self, client_socket: socket.socket) -> None:
        """Handle client connections

        Args:
            client_socket (socket.socket): client socket
        """
        try:
            data = client_socket.recv(1024)
        except OSError:
            return
        if not data:
            return

        msg = Message.from_bytes(data)

        if msg.type == MessageTypes.HEARTBEAT:
            # Update cluster info
            self.leader_host_port = (msg.dest_host, msg.dest_port)
            self.peers = set(
                [tuple(p) for p in msg.all_nodes if p != [self.host, self.port]]
            )
        elif msg.type == MessageTypes.PUBLISH:
            # pick one from the set, and break
            for p in self.peers:
                self.forward_to(msg, p)
                break
        elif msg.type == MessageTypes.SUBSCRIBE or msg.type == MessageTypes.UNSUBSCRIBE:
            # forward to the leader
            self.forward_to(msg, self.leader_host_port)

    def forward_to(self, msg: Message, host_port: Tuple[str, int]):
        """Forward message to leader, usually a `write` message like SUBSCRIBE"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.logger.info(f"Forward {msg.type.name} to {host_port}")
            s.connect(host_port)
            s.sendall(msg.to_bytes())

    def accept_client_connections(self) -> None:
        """Accept connections from clients (Publisher / Subscriber)"""
        while not self.stopped:
            try:
                # Accept new client connections and start a new thread to handle each client
                client_socket, _ = self.server_socket.accept()
                Thread(
                    target=self.handle_client,
                    args=(client_socket,),
                    daemon=True,
                ).start()
            except ConnectionAbortedError:
                # when `self.stop()` is invoked, it closes server_socket and yields this exception
                break

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        self.logger.info(f"Running on {self.host}:{self.port}")
        # reuse to avoid OSError: [Errno 48] Address already in use
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)

        # Start accepting client connections
        Thread(target=self.accept_client_connections).start()

    def stop(self):
        """Stop the broker properly to avoid error `OSError: [Errno 48] Address already in use`"""
        self.stopped = True
        self.server_socket.close()
