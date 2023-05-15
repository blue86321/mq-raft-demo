from enum import Enum
from typing import Union


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
    SEPARATOR = ":"
    NESTED_MSG_SEPARATOR = "|"

    def __init__(
        self,
        type: MessageTypes,
        topic: str = "",
        content: str = "",
        dest_host: str = "",
        dest_port: str = "",
        election_term: str = "",
        nested_msg: Union["Message", None] = None,
    ):
        self.type = type
        self.topic = topic
        self.content = content
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.election_term = election_term
        self.nested_msg = nested_msg

    def to_bytes(self) -> bytes:
        """Convert a message to bytes to send via a socket"""
        data_list = [
            str(self.type.value),
            str(self.topic),
            str(self.content),
            str(self.dest_host),
            str(self.dest_port),
            str(self.election_term),
        ]
        # nested message encoding
        encoded_msg = "None".encode()
        if self.nested_msg:
            encoded_msg = self.nested_msg.to_bytes()
        # bytes
        b_arr = bytearray(
            (self.SEPARATOR.join(data_list) + self.NESTED_MSG_SEPARATOR).encode()
        )
        b_arr.extend(encoded_msg)
        return bytes(b_arr)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Convert bytes received from a socket to a message object"""
        decoded_data = data.decode()
        primary, nested = decoded_data.split(cls.NESTED_MSG_SEPARATOR, 1)
        primary_split = primary.split(cls.SEPARATOR)
        # nested message decoding
        nested_res = None
        if nested != "None":
            nested_res = Message.from_bytes(nested.encode())
        return cls(MessageTypes(primary_split[0]), *primary_split[1:], nested_res)
