from enum import Enum


BROKER_HOST = "localhost"
BROKER_PORT = 8000

SUBSCRIBER_HOST = "localhost"
SUBSCRIBER_PORT = 9000


class NodeState(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class MessageTypes(Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    HEARTBEAT = "heartbeat"
    ACK = "ack"
    REQUEST_TO_VOTE = "request_to_vote"
    VOTE = "vote"


class Message:
    def __init__(
        self,
        type: MessageTypes,
        topic: str = "",
        content: str = "",
        dest_host: str = "",
        dest_port: str = "",
        election_term: str = "",
    ):
        self.type = type
        self.topic = topic
        self.content = content
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.election_term = election_term

    def to_bytes(self) -> bytes:
        """Convert a message to bytes for sending over a socket"""
        return f"{self.type.value}:{self.topic}:{self.content}:{self.dest_host}:{self.dest_port}:{self.election_term}".encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Convert bytes received over a socket to a message object"""
        split_data = data.decode().split(":")
        return cls(MessageTypes(split_data[0]), *split_data[1:])