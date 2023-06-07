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
    time.sleep(2)

    topic = "topic1"
    print("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber(*host_ips[0])
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    print("\n\n==================== Node Join ====================")
    new_node_ip = (host_ips[1][0], host_ips[1][1] + 5)
    broker3 = Broker(
        host=new_node_ip[0],
        port=new_node_ip[1],
        join_dest=host_ips[1],
        election_timeout=1,
    )
    broker3.run()
    time.sleep(2)

    print("\n\n==================== Publish ====================")
    # publisher
    publisher = Publisher(*new_node_ip)
    publisher.publish(topic, "Hello, world!")
    time.sleep(1)

    print("\n\n==================== Node Leave ====================")
    broker3.stop()
    time.sleep(2)

    # stop
    subscriber.stop()
    for broker in brokers:
        broker.stop()
    cluster.stop()


if __name__ == "__main__":
    main()
