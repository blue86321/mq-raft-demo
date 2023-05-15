import logging
from queue import Queue
import random
import socket
from threading import Thread, Timer
import threading
import time
from typing import Callable, List, Tuple

from utils import Message, MessageTypes, NodeState


class RaftNode:
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
        self.peers: List[Tuple[str, int]] = peers or []
        self.peer_sockets: List[socket.socket] = []
        self.majority = (len(self.peers) + 1) / 2
        self.leader_host_port: Tuple(str, int) = None

        # append entries (log replication)
        self.local_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.sent_entry_queue: Queue[Tuple[Message, Callable]] = Queue()
        self.entry_ack_nodes = 0
        # buffer queue for follower to keep entries
        self.buffer_queue: Queue[Message] = Queue()

        self.stopped = False

    def set_stopped(self, value: bool):
        self.stopped = value

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
                msg = Message(
                    MessageTypes.REQUEST_TO_VOTE,
                    election_term=str(self.election_term),
                )
                for peer_host, peer_port in self.peers:
                    Thread(
                        target=self.__send_request_to_vote,
                        args=(peer_host, peer_port, msg),
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
                msg = Message(MessageTypes.VOTE)
                client_socket.sendall(msg.to_bytes())
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

        # update leader info
        leader_host = msg.dest_host
        leader_port = int(msg.dest_port)
        self.leader_host_port = (leader_host, leader_port)

    def handle_append_entries(self, append_entries: Message):
        """Override to handle incoming append_entries from the leader"""
        raise Exception(
            "Please override this method to handle incoming append_entries form the leader"
        )

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
        # todo: no vote
        except ConnectionRefusedError | socket.timeout:
            pass

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
                    dest_port=str(self.port),
                    # send with heartbeat
                    nested_msg=nested_msg,
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
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(self.HEARTBEAT_TIMEOUT)
                s.connect((peer_host, peer_port))
                self.logger.debug(f"Send heartbeat to peer {peer_host}:{peer_port}")
                # wait for ACK
                s.sendall(msg.to_bytes())
                data = s.recv(1024)
                if not data:
                    return
                ack_msg = Message.from_bytes(data)
                self.logger.debug(
                    f"Received {ack_msg.type.name} from {peer_host}:{peer_port}"
                )
                # append_entries
                if msg.nested_msg:
                    self.__check_append_entries()
        # todo: peer fails
        except ConnectionRefusedError:
            pass
        except socket.timeout:
            pass

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
            while not self.stopped:
                data = s.recv(1024)
                if not data:
                    return
                client_socket.sendall(data)

    @property
    def is_leader(self):
        return self.state == NodeState.LEADER

    @property
    def election_timeout(self):
        """Timeout for election in millisecond. If timeout reached, node becomes a candidate"""
        return self.predefined_election_timeout * 1000 or (
            self.HEARTBEAT_INTERVAL * 1000 + random.random() * 150
        )

    def setup_append_entries(self, append_entries: Message, callback: Callable):
        """Setup append_entries. This message will send to peers in the next heartbeat"""
        if len(self.peers) == 0:
            # if no peers, simply put to `sent_entry_queue`
            self.sent_entry_queue.put((append_entries, callback))
        else:
            self.local_entry_queue.put((append_entries, callback))
        self.__check_append_entries(self_check=True)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state
        self.logger = logging.getLogger(
            f"{self.__class__.__name__} {self.port} {self._state.name}"
        )

    def run(self):
        self.__reset_election_timeout()
