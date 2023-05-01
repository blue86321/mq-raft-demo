from enum import Enum


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8888

class NodeState(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class MessageTypes(Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    HEARTBEAT = "heartbeat"
    ACK = "ack"
    REQUEST_TO_VOTE = "request_to_vote"
    VOTE = "vote"


class Message:
    def __init__(self, type: MessageTypes, topic: str = "", content: str = ""):
        self.type = type
        self.topic = topic
        self.content = content

    def to_bytes(self) -> bytes:
        """Convert a message to bytes for sending over a socket"""
        return f"{self.type.value}:{self.topic}:{self.content}".encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Convert bytes received over a socket to a message object"""
        type, topic, content = data.decode().split(":", 2)
        return cls(MessageTypes(type), topic, content)
