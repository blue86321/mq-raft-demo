import logging
import socket
from threading import Thread
from typing import Dict, List, Set, Tuple
from src.raft import RaftNode

from src.utils import BROKER_HOST, BROKER_PORT, CLUSTER_HOST, CLUSTER_PORT, Message, MessageTypes


class Broker(RaftNode):
    def __init__(
        self,
        host: str = BROKER_HOST,
        port: int = BROKER_PORT,
        backlog: int = 5,
        # Whether this node is to join an existing cluster
        join = False,
        # cluster manager
        manager: Tuple[str, int] = (CLUSTER_HOST, CLUSTER_PORT),
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = 0,
    ):
        RaftNode.__init__(self, host, port, manager, peers, election_timeout)
        self.backlog = backlog
        self.join = join
        # Store subscribed clients for each topic
        #  e.g. { 'topic': set((host1, port1), (host2, port2), ...) }
        self.topic_subscribers: Dict[str, Set[Tuple[str, int]]] = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    @property
    def sync_data(self):
        return self.topic_subscribers

    def handle_sync(self, msg: Message, client_socket: socket.socket):
        """For new node joins the cluster"""
        # Sync all `topic_subscribers` received from the leader
        if msg.sync_data:
            self.topic_subscribers = {
                topic: set([tuple(host) for host in host_list])
                for topic, host_list in msg.sync_data.items()
            }
        # start node election
        super().after_handle_sync(msg, client_socket)

    def handle_publish(self, msg: Message):
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

    def handle_subscribe(self, msg: Message):
        if msg.topic not in self.topic_subscribers:
            # Create a new set to store subscribed clients for this topic
            self.topic_subscribers[msg.topic] = set()
        host_port = (msg.dest_host, msg.dest_port)
        self.topic_subscribers[msg.topic].add(host_port)

    def handle_unsubscribe(self, msg: Message):
        if msg.topic in self.topic_subscribers:
            host_port = (msg.dest_host, msg.dest_port)
            self.topic_subscribers[msg.topic].discard(host_port)

    def handle_append_entries(self, entry: Message):
        """Handle append_entries message
        For `Leader`: will execute when majority nodes ACK to the leader
        For `Follower`: will store to buffer at first, then send ACK to the leader. Call the method at next heartbeat from the leader.
        """
        self.logger.info(
            f"Handle append_entries: {entry.type.name} on topic: `{entry.topic}` from {entry.dest_host}:{entry.dest_port}"
        )
        if entry.type == MessageTypes.SUBSCRIBE:
            self.handle_subscribe(entry)
        elif entry.type == MessageTypes.UNSUBSCRIBE:
            self.handle_unsubscribe(entry)

        # leader: send ACK to the subscriber
        if self.is_leader:
            host_port = (entry.dest_host, entry.dest_port)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(host_port)
                    s.sendall(
                        Message(
                            MessageTypes.ACK,
                            topic=entry.topic,
                            content=entry.type.value,
                        ).to_bytes()
                    )
            except ConnectionRefusedError:
                pass

    def handle_client(self, client_socket: socket.socket) -> None:
        """Handle client connections, including
            1. broker operation like UN/SUBSCRIBE, PUBLISH,
            2. cluster message like HEARTBEAT, REQUEST_TO_VOTE

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

        if msg.type == MessageTypes.PUBLISH:
            # Handle PUBLISH message
            self.handle_publish(msg)
        elif msg.type == MessageTypes.SUBSCRIBE or msg.type == MessageTypes.UNSUBSCRIBE:
            # Handle UN/SUBSCRIBE message
            self.handle_update(msg)
        elif msg.type == MessageTypes.HEARTBEAT:
            self.on_receive_heartbeat(client_socket, msg)
        elif msg.type == MessageTypes.REQUEST_TO_VOTE:
            self.handle_request_to_vote(client_socket, msg)
        elif msg.type == MessageTypes.JOIN_CLUSTER:
            self.handle_join_cluster(msg)
        elif msg.type == MessageTypes.SYNC_DATA:
            self.handle_sync(msg, client_socket)

    def run(self) -> None:
        """Start the broker and listen for client connections"""
        self.logger.info(f"Running on {self.host}:{self.port}")
        # reuse to avoid OSError: [Errno 48] Address already in use
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.backlog)

        if self.join:
            # join an existing cluster
            super().request_join_cluster((self.host, self.port), self.manager)
        else:
            # RaftNode run (leader election)
            super().run()

        # Start accepting client connections
        Thread(target=self.accept_client_connections).start()

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
