from abc import ABC, abstractmethod
import logging
import random
import socket
import threading
import time
from queue import Queue
from threading import Thread, Timer
from typing import Callable, List, Tuple

from src.utils import Message, MessageTypes, NodeState


class BaseNode(ABC):
    def __init__(
        self,
        host: str,
        port: int,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
    ):
        self.host = host
        self.port = port

        self.lock = threading.RLock()

        # cluster info
        self.peers = set(peers) if peers else set()
        self.leader_host_port: Tuple[str, int] = None

        self.stopped = False

        self.logger: logging.Logger = None

    @property
    def all_nodes(self):
        """ALl nodes in the cluster"""
        all_peers = set()
        all_peers.add((self.host, self.port))
        all_peers.update(self.peers)
        return all_peers

    @property
    def majority(self):
        return int((len(self.peers) + 1) / 2)

    @abstractmethod
    def get_append_entries_for_heartbeat(self) -> Message:
        """`LogReplication` will override method for `LeaderElection` to use"""
        pass
    
    @abstractmethod
    def get_leader_commit(self) -> Message:
        """`LogReplication` will override method for `LeaderElection` to use"""
        pass

    @abstractmethod
    def check_majority_append_entries(self, msg: Message, self_check=False):
        """`LogReplication` will override method for `LeaderElection` to use"""
        pass


class LeaderElection(BaseNode, ABC):
    """Leader Election
    If a follower does not receive a heartbeat from the leader before its timeout expires, it becomes a candidate.
    The candidate then sends a REQUEST_TO_VOTE message to all nodes in the cluster. Each node can only vote once in a given election term.
    The candidate that receives the majority of votes becomes the new leader and continues to send heartbeats to all nodes regularly.
    If no candidate receives the majority of votes before the timeout expires, a new election term begins.
    """
    HEARTBEAT_INTERVAL = 0.15  # seconds
    HEARTBEAT_TIMEOUT = 3  # seconds

    def __init__(
        self,
        host: str,
        port: int,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = 0,
    ):
        BaseNode.__init__(self, host, port, peers)
        self._state: NodeState = None
        self.state = NodeState.FOLLOWER

        self.election_term = 0
        self.voted = False
        self.vote_count = 0

        # timeout
        self.predefined_election_timeout = election_timeout
        self.timer: Timer = None

    @property
    def election_timeout(self):
        """Timeout for election in millisecond. If timeout reached, node becomes a candidate"""
        return self.predefined_election_timeout * 1000 or (
            self.HEARTBEAT_INTERVAL * 1000 + random.random() * 150
        )

    @property
    def is_leader(self):
        """Check if the current node is a leader"""
        return self.state == NodeState.LEADER

    @property
    def state(self):
        """Node state (e.g. FOLLOWER, LEADER)"""
        return self._state

    @state.setter
    def state(self, state: NodeState):
        self._state = state
        self.logger = logging.getLogger(
            f"{self.__class__.__name__} {self.port} {self._state.name}"
        )

    def start_election(self):
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
        except ConnectionRefusedError:
            # view a node as leaving if it refuses connection while voting
            with self.lock:
                if (peer_host, peer_port) in self.peers:
                    self.logger.info(f"Node leave the cluster: {peer_host}:{peer_port}")
                    self.peers.remove((peer_host, peer_port))
        finally:
            # check convert_to_leader no matter ACK or remove nodes
            if self.state == NodeState.CANDIDATE:
                self.__check_convert_to_leader()

    def __reset_vote(self):
        self.voted = False
        self.vote_count = 0

    def __check_convert_to_leader(self):
        """become leader when receiving a majority of votes"""
        if self.vote_count > self.majority and self.state != NodeState.LEADER:
            self.__reset_vote()
            self.state = NodeState.LEADER
            self.leader_host_port = (self.host, self.port)
            self.logger.info(f"New leader {self.host}:{self.port}")

            self.__reset_election_timeout()
            Thread(target=self.__send_heartbeats).start()

    def __send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        while not self.stopped and self.state == NodeState.LEADER:
            with self.lock:
                self.entry_ack_nodes = 1
                nested_msg = self.get_leader_commit()
                if not nested_msg:
                    nested_msg = self.get_append_entries_for_heartbeat()
                msg = Message(
                    MessageTypes.HEARTBEAT,
                    dest_host=self.host,
                    dest_port=self.port,
                    # send with heartbeat
                    nested_msg=nested_msg,  # append entries
                    all_nodes=self.all_nodes,  # cluster info
                )
                for peer_host, peer_port in self.peers:
                    Thread(
                        target=self.__send_one_heartbeat,
                        args=(peer_host, peer_port, msg),
                    ).start()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def __send_one_heartbeat(
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
            except (ConnectionRefusedError, socket.timeout):
                # mark the node leave if get refused or timeout
                with self.lock:
                    if (peer_host, peer_port) in self.peers:
                        self.logger.info(f"Node leave the cluster: {peer_host}:{peer_port}")
                        self.peers.remove((peer_host, peer_port))
            finally:
                # check append_entries no matter ACK or remove nodes
                if msg.nested_msg:
                # if msg.nested_msg and msg.nested_msg.type != MessageTypes.COMMIT:
                    self.check_majority_append_entries(msg)

    def handle_request_to_vote(self, client_socket: socket.socket, msg: Message):
        """Vote a node to be next possible leader
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
                self.logger.info(
                    f"Vote to leader {msg.dest_host}:{msg.dest_port}, term: {self.election_term}"
                )
                # reset timer
                self.__reset_election_timeout()

    def __update_cluster_info(self, msg: Message):
        """Update cluster info based on msg from the leader, so that current node has a big picture of the cluster"""
        leader_host = msg.dest_host
        leader_port = msg.dest_port
        self.leader_host_port = (leader_host, leader_port)
        self.peers = set(
            [tuple(p) for p in msg.all_nodes if p != [self.host, self.port]]
        )

    def on_receive_heartbeat(
        self,
        client_socket: socket.socket,
        msg: Message,
    ):
        """Receive heartbeat, reset countdown timer"""
        self.logger.debug(
            f"Received heartbeat from {msg.dest_host}:{msg.dest_port}, send ACK back"
        )
        self.__reset_vote()
        self.__reset_election_timeout()

        # send ACK back to the leader
        client_socket.sendall(Message(MessageTypes.ACK).to_bytes())
        # update cluster info
        self.__update_cluster_info(msg)

    def forward_to_leader(self, msg: Message):
        """Forward message to leader, usually a `write` message like SUBSCRIBE"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.logger.info(
                f"Forward {msg.type.name} to leader: {self.leader_host_port}"
            )
            s.connect(self.leader_host_port)
            s.sendall(msg.to_bytes())


class LogReplication(BaseNode, ABC):
    """Log Replication
    Log replication occurs when there is an update request to the cluster.

    When followers receive an update request, they forward it to the leader.
    When a leader receives an update request, it keeps the request locally and sends it to all nodes in the cluster.

    Followers receive an update request from the leader, store the request in a local buffer, and reply with an ACK.
    When the majority of nodes ACK, the leader updates its local copy and sends an ACK to the client.
    Followers update their local copies during the next heartbeat.

    The overall concept is similar to the two-phase commit (2PC) protocol.
    """

    def __init__(self):
        # append entries (log replication)
        self.local_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.sent_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.entry_ack_nodes = 0
        # buffer queue for follower to keep entries
        self.buffer_queue: Queue[Message] = Queue()
        self._leader_commit = None

    def get_leader_commit(self):
        """Get commit msg to send to all follower nodes that the `append_entries` can be flushed into disk"""
        ret = self._leader_commit
        self._leader_commit = None
        return ret
    
    def set_leader_commit(self):
        with self.lock:
            self._leader_commit = Message(MessageTypes.LEADER_COMMIT)

    @abstractmethod
    def handle_append_entries(self, entry: Message):
        """Override to handle incoming append_entries from the leader"""
        raise Exception(
            "Please override this method to handle incoming append_entries form the leader"
        )

    def get_append_entries_for_heartbeat(self):
        """Leader sends `append_entries` to followers along with heartbeat"""
        if not self.local_entry_queue.empty():
            msg, callback = self.local_entry_queue.get()
            self.sent_entry_queue.put((msg, callback))
            return msg
        return None

    def on_receive_heartbeat(self, msg: Message):
        """Append entries to local when receiving a heartbeat"""
        if msg.nested_msg:
            if msg.nested_msg.type == MessageTypes.LEADER_COMMIT:
                self.logger.info(f"Received {msg.nested_msg.type.name}, persist data")
                while not self.buffer_queue.empty():
                    # confirmation received, empty buffer
                    self.handle_append_entries(self.buffer_queue.get())
            else:
                # store in buffer, wait for `LEADER_COMMIT` as a confirmation
                # similar to 2PC protocol
                self.logger.info("Received append_entries, store in buffer")
                self.buffer_queue.put(msg.nested_msg)

    def check_majority_append_entries(self, msg: Message, self_check=False):
        """For leader, if majority of peers ACK, append entries on leader node

        Args:
            msg (Message): Message that leader send to followers, `append_entries` is in `nested_msg` field
            self_check (bool, optional): Whether this method is called by leader node itself. Defaults to False.
        """
        # only increment when it is called because of an ACK from peers
        if not self_check:
            self.entry_ack_nodes += 1

        # check majority
        if self.entry_ack_nodes > self.majority and not self.sent_entry_queue.empty():
            self.logger.info(f"Majority {MessageTypes.ACK.name}, append entries")
            while not self.sent_entry_queue.empty():
                msg, callback = self.sent_entry_queue.get()
                if callback:
                    callback(msg)
            self.set_leader_commit()

    def setup_append_entries(self, append_entries: Message, callback: Callable):
        """Setup append_entries. This message will send to peers in the next heartbeat"""
        if len(self.peers) == 0:
            # if no peers, simply put to `sent_entry_queue`
            self.sent_entry_queue.put((append_entries, callback))
        else:
            self.local_entry_queue.put((append_entries, callback))
        self.check_majority_append_entries(append_entries, self_check=True)


class DynamicMembership(BaseNode, ABC):
    """Dynamic Membership
    Leader sends cluster information to all nodes in the cluster at each heartbeat.

    JOIN:
        If a new node wants to join the cluster, it should send a JOIN_CLUSTER request to one node in the cluster.
        When followers receive a JOIN_CLUSTER request, they forward it to the leader.
        When a leader receives a JOIN_CLUSTER request, it will send a SYNC_DATA message to the new node.
        The new node updates its local data to synchronize with the leader and responds with an ACK.
        Upon receiving the ACK, the leader adds the new node to the cluster and notifies all nodes in the cluster during the next heartbeat.

    LEAVE:
        Two situations will be regarded as a node leaving for a leader:
            1. A node does not ACK the leader's heartbeat before the HEARTBEAT_TIMEOUT.
            2. A node closes the socket, resulting in a `refuse to connect` from the node.
        In either case, the leader updates the cluster information and sends it to all nodes in the cluster.
    """

    def __init__(self):
        pass

    @property
    def sync_data(self):
        raise NotImplementedError("Please implement sync_data")

    def request_join_cluster(self, source: Tuple[str, int], dest: Tuple[str, int]):
        """Send a request to join a cluster to the destination node"""
        Thread(
            target=self.__request_join_cluster,
            args=(source, dest),
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

    @abstractmethod
    def handle_sync(self, msg: Message, client_socket: socket.socket):
        raise Exception(
            "Please override this method, in the end call `after_handle_sync`"
        )

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
        self.start_election()

    @abstractmethod
    def start_election(self):
        raise Exception("Please override this method")


class RaftNode(LeaderElection, LogReplication, DynamicMembership, ABC):
    def __init__(
        self,
        host: str,
        port: int,
        # list of peer host and port
        peers: List[Tuple[str, int]] = None,
        election_timeout: float = 0,
    ):
        LeaderElection.__init__(self, host, port, peers, election_timeout)
        LogReplication.__init__(self)
        DynamicMembership.__init__(self)

    def on_receive_heartbeat(
        self,
        client_socket: socket.socket,
        msg: Message,
    ):
        """Receive heartbeat, reset countdown timer
        """
        self.logger.debug(
            f"Received heartbeat from {msg.dest_host}:{msg.dest_port}, send ACK back"
        )
        LeaderElection.on_receive_heartbeat(self, client_socket, msg)
        LogReplication.on_receive_heartbeat(self, msg)

    def handle_join_cluster(self, msg: Message):
        """Leader need to handle the request to join a node in the cluster"""
        if self.is_leader:
            DynamicMembership.handle_join_cluster(self, msg)
        else:
            # Forward to leader if current node is not a leader
            self.forward_to_leader(msg)

    def handle_update(self, msg: Message):
        """Update operation needs to forward to leader to make sure majority nodes agree on executing the update"""
        if self.is_leader:
            self.logger.info(
                f"New {msg.type.name} to `{msg.topic}` from {msg.dest_host}:{msg.dest_port}"
            )
            self.setup_append_entries(msg, self.handle_append_entries)
        else:
            # Forward to leader if current node is not a leader
            self.forward_to_leader(msg)

    def set_stopped(self, value: bool):
        self.stopped = value

    def run(self):
        self.start_election()
