import logging
import threading
import time
from typing import List

from broker import Broker
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT
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

    broker1 = Broker(host=host_ips[0][0], port=host_ips[0][1], peers=[host_ips[1]], election_timeout=3)
    brokers.append(broker1)
    broker2 = Broker(host=host_ips[1][0], port=host_ips[1][1], peers=[host_ips[0]], election_timeout=1.5)
    brokers.append(broker2)

    def run_broker(broker: Broker):
        broker.run()

    for broker in brokers:
        threading.Thread(target=run_broker, args=(broker,)).start()
    time.sleep(5)

    # subscriber
    subscriber = Subscriber(*host_ips[0])
    threading.Thread(target=subscriber.receive).start()
    subscriber.subscribe("topic1")
    time.sleep(1)

    # publisher
    publisher = Publisher(*host_ips[1])
    publisher.publish("topic1", "Hello, world!")
    time.sleep(1)

    # stop
    subscriber.stop()
    for broker in brokers:
        broker.stop()


if __name__ == "__main__":
    main()
