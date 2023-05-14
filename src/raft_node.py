import datetime
import logging
import random
import socket
from threading import Thread, Timer
import threading
import time
from typing import List, Tuple

from utils import Message, MessageTypes, NodeState


class RaftNode:
    HEARTBEAT_INTERVAL = 0.5  # seconds
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
        self.logger = logging.getLogger(f"{__name__} {self.port}")

        self.state = NodeState.FOLLOWER
        self.election_term = 0
        self.voted = False
        self.vote_count = 0
        # timeout
        self.predefined_election_timeout = election_timeout
        self.lock = threading.RLock()
        self.timer = None
        self.__reset_timeout()
        # cluster info
        self.peers: List[Tuple[str, int]] = peers or []
        self.peer_sockets: List[socket.socket] = []
        self.leader_host_port = None

        self.stopped = False

    def set_stopped(self, value: bool):
        self.stopped = value

    def convert_to_candidate(self):
        """
        If no heartbeat coming until timer expires,
        current node convert to a candidate and sends REQUEST_TO_VOTE to other nodes.
        """
        with self.lock:
            self.election_term += 1
            self.logger.info(
                f"Timeout, sending {MessageTypes.REQUEST_TO_VOTE.name}, term: {self.election_term}"
            )
            # vote itself
            self.voted = True
            self.vote_count = 1
            self.state = NodeState.CANDIDATE
            self.check_convert_to_leader()

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
            self.__reset_timeout()

    def reset_vote(self):
        self.voted = False
        self.vote_count = 0

    def check_convert_to_leader(self):
        """become leader when receiving a majority of votes"""
        if self.vote_count > (len(self.peers) + 1) / 2:
            self.logger.info(f"New leader {self.host}:{self.port}")
            self.reset_vote()
            self.state = NodeState.LEADER
            self.__reset_timeout()
            Thread(target=self.send_heartbeats).start()

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
                self.__reset_timeout()

    def __reset_timeout(self):
        if isinstance(self.timer, Timer):
            self.timer.cancel()
        if self.state != NodeState.LEADER:
            self.timer = Timer(self.election_timeout / 1000, self.convert_to_candidate)
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
        self.reset_vote()
        self.__reset_timeout()

        # send ACK
        client_socket.sendall(Message(MessageTypes.ACK).to_bytes())

        leader_host = msg.dest_host
        leader_port = int(msg.dest_port)
        self.leader_host_port = (leader_host, leader_port)

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
                    self.check_convert_to_leader()
        # todo: no vote
        except ConnectionRefusedError:
            pass
        except socket.timeout:
            pass

    def send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        while not self.stopped and self.state == NodeState.LEADER:
            msg = Message(
                MessageTypes.HEARTBEAT, dest_host=self.host, dest_port=str(self.port)
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
                s.sendall(msg.to_bytes())
                data = s.recv(1024)
                if not data:
                    return
                msg = Message.from_bytes(data)
                self.logger.debug(f"Received ACK from {peer_host}:{peer_port}")
        # todo: peer fails
        except ConnectionRefusedError:
            pass
        except socket.timeout:
            pass

    def forward_to_leader(self, client_socket: socket.socket, msg: Message):
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
        return self.predefined_election_timeout * 1000 or (
            self.HEARTBEAT_INTERVAL * 1000 + random.random() * 150
        )
