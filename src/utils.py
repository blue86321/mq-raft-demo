from enum import Enum


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8888


class MessageTypes(Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"


class Message:
    def __init__(self, type: MessageTypes, topic: str, content: str):
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
