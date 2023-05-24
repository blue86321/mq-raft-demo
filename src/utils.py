from enum import Enum
import json
from typing import Dict, List, Set, Tuple, Union


BROKER_HOST = "localhost"
BROKER_PORT = 8000

SUBSCRIBER_HOST = "localhost"
SUBSCRIBER_PORT = 9000


class NodeState(str, Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class MessageTypes(str, Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    HEARTBEAT = "heartbeat"
    ACK = "ack"
    REQUEST_TO_VOTE = "request_to_vote"
    VOTE = "vote"
    JOIN_CLUSTER = "join_cluster"
    SYNC_DATA = "sync_data"


class Message:
    def __init__(
        self,
        type: Union[MessageTypes, str],
        topic: str = "",
        content: str = "",
        dest_host: str = "",
        dest_port: str = "",
        election_term: str = "",
        nested_msg: Union["Message", None] = None,
        sync_data: Dict[
            str, Union[Set[Tuple[str, int]], List[List[Union[str, int]]]]
        ] = None,
        all_nodes: Union[Set[Tuple[str, int]], List[List[Union[str, int]]]] = None,
    ):
        self.type = type if isinstance(type, MessageTypes) else MessageTypes(type)
        self.topic = topic
        self.content = content
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.election_term = election_term
        self.nested_msg = nested_msg
        self.sync_data = (
            {k: list(v) for k, v in sync_data.items()} if sync_data else None
        )
        self.all_nodes = list(all_nodes) if all_nodes else None

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    @classmethod
    def from_json(cls, data):
        return json.loads(data, object_hook=cls.from_dict)

    def to_bytes(self) -> bytes:
        """Convert a message to bytes to send via a socket"""
        return bytes(self.to_json().encode())

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Convert bytes received from a socket to a message object"""
        decoded_data = data.decode()
        instance: Message = cls.from_json(decoded_data)
        instance.type = MessageTypes(instance.type)
        return instance

    @classmethod
    def from_dict(cls, dict: Dict) -> "Message":
        """Deserialize from a nested object"""
        
        if dict.get("type"):
            # if type (a str format of MessageType) exists, convert it
            obj = cls(dict.pop("type"))
            obj.__dict__.update(dict)
            return obj
        else:
            # otherwise return dict directly (e.g. dict object like `sync_data`)
            return dict
