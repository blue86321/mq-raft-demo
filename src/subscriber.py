import logging
import socket
import threading
from utils import (
    BROKER_HOST,
    BROKER_PORT,
    SUBSCRIBER_HOST,
    SUBSCRIBER_PORT,
    Message,
    MessageTypes,
)


class Subscriber:
    def __init__(
        self,
        broker_host: str = BROKER_HOST,
        broker_port: int = BROKER_PORT,
        subscriber_host: str = SUBSCRIBER_HOST,
        subscriber_port: int = SUBSCRIBER_PORT,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.subscriber_host = subscriber_host
        self.subscriber_port = subscriber_port

        self.recv_socket = None

        self.logger = logging.getLogger(
            f"{self.__class__.__name__} {self.subscriber_port}"
        )

    def subscribe(self, topic: str) -> None:
        """Subscribe to a topic at the broker

        Args:
            topic (str): message topic to subscribe to
        """
        # Create a message object with type SUBSCRIBE and the specified topic
        msg = Message(
            MessageTypes.SUBSCRIBE,
            topic,
            "",
            self.subscriber_host,
            self.subscriber_port,
        )
        self.logger.info(f"Subscribe message on topic `{msg.topic}`")
        # Send the message to the broker via a socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.broker_host, self.broker_port))
            s.sendall(msg.to_bytes())

    def receive(self) -> None:
        """Receive message from the broker"""
        self.logger.info(
            f"Subscriber is running on {self.subscriber_host}:{self.subscriber_port}"
        )
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket.bind((self.subscriber_host, self.subscriber_port))
        self.recv_socket.listen(5)
        while True:
            try:
                client_socket, address = self.recv_socket.accept()
                data = client_socket.recv(1024)
                if not data:
                    continue
                # Convert the received bytes to a message object
                msg = Message.from_bytes(data)
                self.logger.info(
                    f"Received message: {msg.content} on topic `{msg.topic}`"
                )
            # when self.stop() is invoked, it closes socket and yields this exception
            except OSError:
                break

    def stop(self) -> None:
        """Stop receiving messages from the broker"""
        self.recv_socket.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    subscriber = Subscriber(subscriber_port=SUBSCRIBER_PORT)
    subscriber.subscribe("topic1")
    threading.Thread(target=subscriber.receive).start()