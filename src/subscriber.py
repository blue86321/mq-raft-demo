import logging
import socket
import threading

from utils import DEFAULT_HOST, DEFAULT_PORT, Message, MessageTypes

logger = logging.getLogger(__name__)


class Subscriber:
    def __init__(
        self, broker_host: str = DEFAULT_HOST, broker_port: int = DEFAULT_PORT
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        # use same socket to subscribe and receive
        self.socket = None

    def subscribe(self, topic: str) -> None:
        """Subscribe to a topic at the broker

        Args:
            topic (str): message topic to subscribe to
        """
        # Create a message object with type SUBSCRIBE and the specified topic
        msg = Message(MessageTypes.SUBSCRIBE, topic, "")
        logger.info(f"Subscribe message on topic {msg.topic}")
        # Send the message to the broker over a socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.broker_host, self.broker_port))
        self.socket.sendall(msg.to_bytes())

    def receive(self) -> None:
        """Receive message from the broker"""
        while True:
            try:
                data = self.socket.recv(1024)
                if not data:
                    continue
                # Convert the received bytes to a message object
                msg = Message.from_bytes(data)
                logger.info(f"Received message: {msg.content} on topic {msg.topic}")
            # when self.stop() is invoked, it closes socket and yields this exception
            except OSError:
                break

    def stop(self) -> None:
        """Stop receiving messages from the broker"""
        self.socket.close()


if __name__ == "__main__":
    # log settings
    logger = logging.getLogger(Subscriber.__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    subscriber = Subscriber()
    subscriber.subscribe("topic1")
    threading.Thread(target=subscriber.receive).start()
