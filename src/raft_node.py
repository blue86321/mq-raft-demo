import logging
import random
import socket
import threading
import time
from queue import Queue
from threading import Thread, Timer
from typing import Callable, List, Tuple

from utils import Message, MessageTypes, NodeState


class RaftNode:
    HEARTBEAT_INTERVAL = 0.3    # seconds
    HEARTBEAT_TIMEOUT = 3       # seconds

    # Initialization of RaftNode object
        # Parameters:
        # - host: The host address of the current node
        # - port: The port number of the current node
        # - peers: A list of tuples representing the host and port of other nodes in the cluster (default: None)
        # - election_timeout: The election timeout for the node in seconds (default: 0)
    def __init__(
        self,
        host: str,
        port: int,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = 0.0,
    ):
        self.host = host
        self.port = port

        self._state: NodeState = None
        self.state = NodeState.FOLLOWER

        self.election_term = 0
        self.voted = False
        self.vote_count = 0
        # timeout
        self.predefined_election_timeout = election_timeout
        self.lock = threading.RLock()
        self.timer: Timer = None
        # cluster info
        self.peers = set(peers) if peers else set()
        self.leader_host_port: Tuple(str, int) = None

        # Append entries (log replication)
        self.local_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.sent_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.entry_ack_nodes = 0
        # buffer queue for follower to keep entries
        self.buffer_queue: Queue[Message] = Queue()

        self.stopped = False

    @property
    def all_nodes(self):
        # Returns all nodes in the cluster including the current node
        all_peers = set()
        all_peers.add((self.host, self.port))
        all_peers.update(self.peers)
        return all_peers

    @property
    def majority(self):
        # Returns the majority count required for consensus
        return (len(self.peers) + 1) / 2

    @property
    def is_leader(self):
        # Checks if the current node is the leader
        return self.state == NodeState.LEADER

    @property
    def election_timeout(self):
        # Returns the election timeout for the node in milliseconds
        return self.predefined_election_timeout * 1000 or (
            self.HEARTBEAT_INTERVAL * 1000 + random.random() * 150
        )

    @property
    def state(self):
        # Returns the current state of the node (e.g., FOLLOWER, LEADER)
        return self._state

    @state.setter
    def state(self, state: NodeState):
        # Sets the current state of the node
        self._state = state
        self.logger = logging.getLogger(
            f"{self.__class__.__name__} {self.port} {self._state.name}"
        )

    @property
    def sync_data(self):
        """
        This property needs to be implemented.
        It should return the data that needs to be synchronized with new nodes joining the cluster.
        
        """
        raise NotImplementedError("Please implement sync_data")

    def set_stopped(self, value: bool):
        # Sets the `stopped` flag of the node
        self.stopped = value

    #methods for managing the node's state and behavior

    def __convert_to_candidate(self):
        """
        If no heartbeat coming until timer expires,
        current node convert to a candidate and sends REQUEST_TO_VOTE to other nodes.
        """
        if not self.stopped:
            with self.lock:
                # vote itself
                self.voted = True
                self.election_term += 1
                self.vote_count = 1
                self.state = NodeState.CANDIDATE

                self.logger.info(
                    f"Timeout, sending {MessageTypes.REQUEST_TO_VOTE.name}, term: {self.election_term}"
                )
                self.__check_convert_to_leader()

                # send request to vote to peers
                request_to_vot_msg = Message(
                    MessageTypes.REQUEST_TO_VOTE,
                    election_term=self.election_term,
                    dest_host=self.host,
                    dest_port=self.port,
                )
                for peer_host, peer_port in self.peers:
                    Thread(
                        target=self.__send_request_to_vote,
                        args=(peer_host, peer_port, request_to_vot_msg),
                    ).start()
                # reset timeout in case more than one candidate in the cluster at the same election_term
                self.__reset_election_timeout()

    def __reset_vote(self):
        self.voted = False
        self.vote_count = 0

    def __check_convert_to_leader(self):
        """become leader when receiving a majority of votes"""
        if self.vote_count > self.majority:
            self.__reset_vote()
            self.state = NodeState.LEADER
            self.logger.info(f"New leader {self.host}:{self.port}")

            self.__reset_election_timeout()
            Thread(target=self.__send_heartbeats).start()

    #methods for handling different types of messages

    def handle_request_to_vote(self, client_socket: socket.socket, msg: Message):
        """Vote a node to be next possible leader

        Args:
            host (str): host to vote on
            port (int): port to vote on
        """
        with self.lock:
            msg_term = int(msg.election_term)
            if self.election_term < msg_term:
                # convert to follower
                self.election_term = msg_term
                self.voted = True
                self.state = NodeState.FOLLOWER
                vote = Message(MessageTypes.VOTE)
                client_socket.sendall(vote.to_bytes())
                self.logger.info(f"Vote to leader {msg.dest_host}:{msg.dest_port}, term: {self.election_term}")
                # reset timer
                self.__reset_election_timeout()

    def __reset_election_timeout(self):
        """Reset election timeout. If the node is a leader, timer will not restart"""
        # cancel
        if isinstance(self.timer, Timer):
            self.timer.cancel()
        # restart
        if self.state != NodeState.LEADER:
            self.timer = Timer(
                self.election_timeout / 1000, self.__convert_to_candidate
            )
            self.timer.start()

    def on_receive_heartbeat(
        self,
        client_socket: socket.socket,
        msg: Message,
    ):
        """Receive heartbeat, reset countdown timer

        Args:
            heartbeat_time (datetime.datetime): received heartbeat time
        """
        self.logger.debug(
            f"Received heartbeat from {msg.dest_host}:{msg.dest_port}, send ACK back"
        )
        self.__reset_vote()
        self.__reset_election_timeout()

        # send ACK
        client_socket.sendall(Message(MessageTypes.ACK).to_bytes())

        # append entries
        while not self.buffer_queue.empty():
            # confirmation heartbeat received, empty buffer
            self.handle_append_entries(self.buffer_queue.get())
        if msg.nested_msg:
            # store in buffer, wait for next `heartbeat` as a confirmation
            self.logger.info("Received append_entries, store in buffer")
            self.buffer_queue.put(msg.nested_msg)

        # update cluster info
        leader_host = msg.dest_host
        leader_port = msg.dest_port
        self.leader_host_port = (leader_host, leader_port)
        self.peers = set([tuple(p) for p in msg.all_nodes if p != (self.host, self.port)])

    def handle_append_entries(self, append_entries: Message):
        """Override to handle incoming append_entries from the leader"""
        raise Exception(
            "Please override this method to handle incoming append_entries form the leader"
        )

    #methods for intercommunications
    def __send_request_to_vote(self, peer_host: str, peer_port: int, msg: Message):
        """Send REQUEST_TO_VOTE to one peer

        Args:
            peer_host (str): peer host
            peer_port (int): peer port
            msg (Message): REQUEST_TO_VOTE message
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(self.HEARTBEAT_TIMEOUT)
                s.connect((peer_host, peer_port))
                self.logger.debug(f"Send REQUEST_TO_VOTE to {peer_host}:{peer_port}")
                s.sendall(msg.to_bytes())
                data = s.recv(1024)
                if not data:
                    return
                msg = Message.from_bytes(data)
                self.logger.debug(f"Be voted by {peer_host}:{peer_port}")
                self.vote_count += 1
                if self.state == NodeState.CANDIDATE:
                    self.__check_convert_to_leader()
        except ConnectionRefusedError:
            # view a node as leaving if it refuses connection while voting
            with self.lock:
                self.logger.info(f"Node leave the cluster: {peer_host}:{peer_port}")
                self.peers.remove((peer_host, peer_port))

    def __send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        while not self.stopped and self.state == NodeState.LEADER:
            with self.lock:
                self.entry_ack_nodes = 1
                nested_msg = None
                if not self.local_entry_queue.empty():
                    msg, callback = self.local_entry_queue.get()
                    self.sent_entry_queue.put((msg, callback))
                    nested_msg = msg
                msg = Message(
                    MessageTypes.HEARTBEAT,
                    dest_host=self.host,
                    dest_port=self.port,
                    # send with heartbeat
                    nested_msg=nested_msg,
                    all_nodes=self.all_nodes,
                )
                for peer_host, peer_port in self.peers:
                    Thread(
                        target=self.__deal_with_one_heartbeat,
                        args=(peer_host, peer_port, msg),
                    ).start()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def __deal_with_one_heartbeat(
        self, peer_host: str, peer_port: int, msg: Message
    ) -> None:
        """Send heartbeat to one peer

        Args:
            peer_host (str): peer host
            peer_port (int): peer port
            msg (Message): message to send (HEARTBEAT)
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.settimeout(self.HEARTBEAT_TIMEOUT)
                s.connect((peer_host, peer_port))
                self.logger.debug(f"Send heartbeat to peer {peer_host}:{peer_port}")
                s.sendall(msg.to_bytes())
                # wait for ACK
                data = s.recv(1024)
                if not data:
                    return
                ack = Message.from_bytes(data)
                self.logger.debug(
                    f"Received {ack.type.name} from {peer_host}:{peer_port}"
                )
                # append_entries
                if msg.nested_msg:
                    self.__check_append_entries()
            except (ConnectionRefusedError, socket.timeout):
                # mark the node leave if get refused or timeout
                with self.lock:
                    self.logger.info(f"Node leave the cluster: {peer_host}:{peer_port}")
                    self.peers.remove((peer_host, peer_port))

    def __check_append_entries(self, self_check=False):
        """If majority of peers ack, append entries on current node (leader)

        Args:
            self_check (bool, optional): Whether this method is called by leader node itself. Defaults to False.
        """
        # only increment when it is called because of an ACK from peers
        if not self_check:
            self.entry_ack_nodes += 1

        # check majority
        if self.entry_ack_nodes > self.majority:
            self.logger.info(f"Majority {MessageTypes.ACK.name}, append entries")
            while not self.sent_entry_queue.empty():
                msg, callback = self.sent_entry_queue.get()
                if callback:
                    callback(msg)

    def forward_to_leader(self, client_socket: socket.socket, msg: Message):
        """Forward message to leader, usually a `write` message like SUBSCRIBE"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.logger.info(
                f"Forward {msg.type.name} to leader: {self.leader_host_port}"
            )
            s.connect(self.leader_host_port)
            s.sendall(msg.to_bytes())

    def setup_append_entries(self, append_entries: Message, callback: Callable):
        """Setup append_entries. This message will send to peers in the next heartbeat"""
        if len(self.peers) == 0:
            # if no peers, simply put to `sent_entry_queue`
            self.sent_entry_queue.put((append_entries, callback))
        else:
            self.local_entry_queue.put((append_entries, callback))
        self.__check_append_entries(self_check=True)

    #methods for managing the node's interaction with the cluster
    def request_join_cluster(self, source: Tuple[str, int], dest: Tuple[str, int]):
        """Send a request to join a cluster to the destination node"""
        Thread(
            target=self.__request_join_cluster,
            args=(
                source,
                dest,
            ),
        ).start()

    def __request_join_cluster(self, source: Tuple[str, int], dest: Tuple[str, int]):
        """Send a request to join a cluster to the destination node"""
        self.peers = set()
        cluster_host, cluster_port = dest
        join_msg = Message(
            MessageTypes.JOIN_CLUSTER, dest_host=source[0], dest_port=source[1]
        )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((cluster_host, cluster_port))
            self.logger.info(f"Request JOIN_CLUSTER {cluster_host}:{cluster_port}")
            s.sendall(join_msg.to_bytes())

    def handle_join_cluster(self, msg: Message):
        """New node wants to join the cluster"""
        host = msg.dest_host
        port = msg.dest_port
        sync_msg = Message(
            MessageTypes.SYNC_DATA,
            dest_host=self.host,
            dest_port=self.port,
            all_nodes=self.all_nodes,
            election_term=self.election_term,
            sync_data=self.sync_data,
        )

        # sync data
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            self.logger.info(f"Sync data with peer {host}:{port}")
            s.sendall(sync_msg.to_bytes())
            # wait for ACK
            data = s.recv(1024)
            if not data:
                return
            ack = Message.from_bytes(data)
            # join the cluster
            self.peers.add((host, port))
            self.logger.info(f"New node joins the cluster {host}:{port}")

    def after_handle_sync(self, msg: Message, client_socket: socket.socket):
        """After setting up sync_data, call this function"""
        # send ACK
        client_socket.sendall(Message(MessageTypes.ACK).to_bytes())
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     s.connect((msg.dest_host, msg.dest_port))
        #     s.sendall(Message(MessageTypes.ACK).to_bytes())
        
        # run node (start election timeout)
        self.election_term = msg.election_term
        self.peers = set([tuple(p) for p in msg.all_nodes])
        RaftNode.run(self)

    def run(self):
        self.__reset_election_timeout()
