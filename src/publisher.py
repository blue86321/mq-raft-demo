import logging
import socket

from utils import DEFAULT_HOST, DEFAULT_PORT, Message, MessageTypes

logger = logging.getLogger(__name__)


class Publisher:
    def __init__(
        self, broker_host: str = DEFAULT_HOST, broker_port: int = DEFAULT_PORT
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port

    def publish(self, topic: str, content: str) -> None:
        """Publish a message to the broker.

        Args:
            topic (str): message topic
            content (str): message content
        """
        # Create a message object with type PUBLISH, the specified topic, and message
        msg = Message(MessageTypes.PUBLISH, topic, content)
        logger.info(f"Publish message on topic {msg.topic}: {msg.content}")

        # Send the message to the broker over a socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            s.sendall(msg.to_bytes())


if __name__ == "__main__":
    # log settings
    logger = logging.getLogger(Publisher.__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    publisher = Publisher()
    publisher.publish("topic1", "Hello, world!")
