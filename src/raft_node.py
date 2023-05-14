import datetime
import logging
import random
import socket
from threading import Thread, Timer
import time
from typing import List, Tuple

from utils import Message, MessageTypes, NodeState


class RaftNode:
    HEARTBEAT_INTERVAL = 2  # seconds
    HEARTBEAT_TIMEOUT = 5  # seconds

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
        self.election_timeout = election_timeout * 1000 or (
            self.HEARTBEAT_INTERVAL * 1000 + random.random() * 3000
        )
        self.last_heartbeat_time = None
        self.countdown_timer = Timer(
            self.election_timeout / 1000, self.__countdown_callback
        )
        self.countdown_timer.start()
        self.peers: List[Tuple[str, int]] = peers or []
        self.peer_sockets: List[socket.socket] = []
        self.active_peer_cnt = len(self.peers)
        self.last_heartbeat_sent = None
        self.stopped = False
        self.leader_host_port = None

    def set_stopped(self, value: bool):
        self.stopped = value

    def vote_self(self):
        self.election_term += 1
        self.voted = True
        self.vote_count += 1
        self.state = NodeState.CANDIDATE
        self.check_become_leader()

    def reset_vote(self):
        self.voted = False
        self.vote_count = 0

    def check_become_leader(self):
        """become leader when receiving a majority of votes"""
        if self.vote_count > self.active_peer_cnt / 2:
            self.logger.info(f"New leader {self.host}:{self.port}")
            self.reset_vote()
            self.state = NodeState.LEADER
            Thread(target=self.send_heartbeats).start()

    def vote(self, client_socket: socket.socket, msg: Message):
        """Vote a node to be next possible leader

        Args:
            host (str): host to vote on
            port (int): port to vote on
        """
        # topic to store election term
        msg_term = int(msg.topic)
        if self.election_term < msg_term or (
            self.election_term == msg_term and not self.voted
        ):
            self.election_term = msg_term
            self.voted = True
            self.state = NodeState.FOLLOWER
            msg = Message(MessageTypes.VOTE)
            client_socket.sendall(msg.to_bytes())

    def on_receive_heartbeat(
        self,
        client_socket: socket.socket,
        msg: Message,
        heartbeat_time: datetime.datetime,
    ):
        """Receive heartbeat, reset countdown timer

        Args:
            heartbeat_time (datetime.datetime): received heartbeat time
        """
        self.reset_vote()
        self.state = NodeState.FOLLOWER
        self.last_heartbeat_time = heartbeat_time

        # reset countdown timer
        self.countdown_timer.cancel()
        self.countdown_timer = Timer(
            self.election_timeout / 1000, self.__countdown_callback
        )
        self.countdown_timer.start()

        # send ACK
        client_socket.sendall(Message(MessageTypes.ACK).to_bytes())

        leader_host = msg.dest_host
        leader_port = int(msg.dest_port)
        self.leader_host_port = (leader_host, leader_port)

    def __countdown_callback(self):
        """
        If no heartbeat coming until timer expires,
        current node becomes a candidate and sends REQUEST_TO_VOTE to other nodes.
        """
        self.logger.info(f"Countdown expired, sending {MessageTypes.REQUEST_TO_VOTE.name}")
        self.vote_self()

        msg = Message(
            MessageTypes.REQUEST_TO_VOTE,
            str(self.election_term),
            "",
        )
        for peer_host, peer_port in self.peers:
            Thread(
                target=self.__send_request_to_vote,
                args=(peer_host, peer_port, msg),
            ).start()

    def __send_request_to_vote(self, peer_host: str, peer_port: int, msg: Message):
        """Send REQUEST_TO_VOTE to one peer

        Args:
            peer_host (str): peer host
            peer_port (int): peer port
            msg (Message): REQUEST_TO_VOTE message
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
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
                    self.check_become_leader()
        except ConnectionRefusedError:
            pass

    def send_heartbeats(self) -> None:
        """Send heartbeat to all peers"""
        while not self.stopped and self.state == NodeState.LEADER:
            msg = Message(
                MessageTypes.HEARTBEAT, dest_host=self.host, dest_port=str(self.port)
            )
            self.active_peer_cnt = 0
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
                self.active_peer_cnt += 1
                # todo: set last_ack_time for this peer
                self.last_heartbeat_sent = time.time()
        except ConnectionRefusedError:
            pass
        except socket.timeout:
            # todo: peer fails
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
