import logging
import time
from typing import List

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.publisher import Publisher
from src.utils import BROKER_HOST, BROKER_PORT
from src.subscriber import Subscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    # broker
    host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1)]
    brokers: List[Broker] = []

    broker1 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1],
        peers=[host_ips[1]],
        election_timeout=0.5,
    )
    brokers.append(broker1)
    broker2 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1],
        peers=[host_ips[0]],
        election_timeout=1,
    )
    brokers.append(broker2)

    print("\n\n==================== Broker ====================")
    cluster = ClusterManager()
    cluster.run()
    for broker in brokers:
        broker.run()
    time.sleep(2.5)

    topic = "topic1"
    print("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    print("\n\n==================== Publish ====================")
    # publisher
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(1)

    print("\n\n==================== Unsubscribe and Publish ====================")
    # unsubscribe
    subscriber.unsubscribe(topic)
    print("\n\n==================== Publish (Temporary inconsistent) ====================")
    publisher.publish(topic, "Hello, too fast")
    time.sleep(0.5)
    print("\n\n==================== Publish Later ====================")
    publisher.publish(topic, "Hello, later")
    time.sleep(1)

    # stop
    subscriber.stop()
    for broker in brokers:
        broker.stop()
    cluster.stop()


if __name__ == "__main__":
    main()
