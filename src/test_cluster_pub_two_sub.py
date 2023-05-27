import logging
import time
from typing import List

from broker import Broker
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT, SUBSCRIBER_PORT
from subscriber import Subscriber

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
    for broker in brokers:
        broker.run()
    time.sleep(2.5)

    topic = "topic1"
    # subscriber
    subscribers = [
        Subscriber(*host_ips[0]),
        Subscriber(*host_ips[1], port=SUBSCRIBER_PORT + 5),
    ]
    for idx, sub in enumerate(subscribers):
        print(f"\n\n==================== Subscribe to Node {idx + 1} ====================")
        sub.run()
        sub.subscribe(topic)
    time.sleep(1)

    print("\n\n==================== Publish to Node 2 ====================")
    # publisher
    publisher = Publisher(*host_ips[1])
    publisher.publish(topic, "Hello, world!")
    time.sleep(1)

    print("\n\n==================== Unsubscribe and Publish ====================")
    # unsubscribe
    subscribers[0].unsubscribe(topic)
    print("\n\n==================== Publish (Temporary inconsistent) ====================")
    publisher.publish(topic, "Hello, too fast")
    time.sleep(0.5)
    print("\n\n==================== Publish Later ====================")
    publisher.publish(topic, "Hello, later")
    time.sleep(1)

    # stop
    for sub in subscribers:
        sub.stop()
    for broker in brokers:
        broker.stop()


if __name__ == "__main__":
    main()
