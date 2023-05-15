import logging
import socket

from utils import BROKER_HOST, BROKER_PORT, Message, MessageTypes


class Publisher:
    def __init__(self, broker_host: str = BROKER_HOST, broker_port: int = BROKER_PORT):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.logger = logging.getLogger(self.__class__.__name__)

    def publish(self, topic: str, content: str) -> None:
        """Publish a message to the broker.

        Args:
            topic (str): message topic
            content (str): message content
        """
        # Create a message object with type PUBLISH, the specified topic, and message
        msg = Message(MessageTypes.PUBLISH, topic, content)
        self.logger.info(
            f"Publish to topic `{msg.topic}`: `{msg.content}` at {self.broker_host}:{self.broker_port}"
        )

        # Send the message to the broker via a socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            s.sendall(msg.to_bytes())


if __name__ == "__main__":
    # log settings
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    publisher = Publisher()
    publisher.publish("topic1", "Hello, world!")
