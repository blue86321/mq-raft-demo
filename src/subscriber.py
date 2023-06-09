import logging
import socket
import threading

from src.utils import (CLUSTER_HOST, CLUSTER_PORT,
                       SUBSCRIBER_HOST, SUBSCRIBER_PORT, Message, MessageTypes)


class Subscriber:
    def __init__(
        self,
        cluster_host: str = CLUSTER_HOST,
        cluster_port: int = CLUSTER_PORT,
        host: str = SUBSCRIBER_HOST,
        port: int = SUBSCRIBER_PORT,
    ):
        self.cluster_host = cluster_host
        self.cluster_port = cluster_port
        self.host = host
        self.port = port

        self.recv_socket = None

        self.logger = logging.getLogger(f"{self.__class__.__name__} {self.port}")

    def subscribe(self, topic: str) -> None:
        self.handle_operation(topic, MessageTypes.SUBSCRIBE)

    def unsubscribe(self, topic: str) -> None:
        self.handle_operation(topic, MessageTypes.UNSUBSCRIBE)

    def handle_operation(self, topic: str, msg_type: MessageTypes) -> None:
        """Handle the operation to send message to the cluster

        Args:
            topic (str): message topic
            msg_type (MessageTypes): message type, like UN/SUBSCRIBE
        """
        self.logger.info(
            f"{msg_type.name} message on topic `{topic}` at {self.cluster_host}:{self.cluster_port}"
        )

        # Create a message
        msg = Message(msg_type, topic, dest_host=self.host, dest_port=self.port)

        # Send the message to the cluster via a socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.cluster_host, self.cluster_port))
            s.sendall(msg.to_bytes())

    def receive(self) -> None:
        """Receive message from the cluster"""
        self.logger.info(f"Subscriber is running on {self.host}:{self.port}")
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # reuse to avoid OSError: [Errno 48] Address already in use
        self.recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.recv_socket.bind((self.host, self.port))
        self.recv_socket.listen(5)
        while True:
            try:
                client_socket, _ = self.recv_socket.accept()
                data = client_socket.recv(1024)
                if not data:
                    continue
                # Convert the received bytes to a message object
                msg = Message.from_bytes(data)
                if msg.type == MessageTypes.ACK:
                    # cluster ACK for a Un/Subscribe request
                    # `msg.content` is the operation (that is, `msg.type`` sent to cluster)
                    self.logger.info(f"Received {msg.type.name}: {msg.content} on topic `{msg.topic}`")
                else:
                    self.logger.info(
                        f"Received message: `{msg.content}` on topic `{msg.topic}`"
                    )
            # when self.stop() is invoked, it closes socket and yields this exception
            except OSError:
                break

    def run(self):
        threading.Thread(target=self.receive).start()

    def stop(self) -> None:
        """Stop receiving messages from the cluster"""
        self.recv_socket.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    subscriber = Subscriber(port=SUBSCRIBER_PORT)
    subscriber.subscribe("topic1")
    threading.Thread(target=subscriber.receive).start()
