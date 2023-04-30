import logging
import threading
import time
from typing import List

from broker import Broker
from publisher import Publisher
from utils import DEFAULT_HOST, DEFAULT_PORT
from subscriber import Subscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    # broker
    host_ips = [(DEFAULT_HOST, DEFAULT_PORT), (DEFAULT_HOST, DEFAULT_PORT + 1)]
    brokers: List[Broker] = []

    broker1 = Broker(host=host_ips[0][0], port=host_ips[0][1], peers=[host_ips[1]])
    brokers.append(broker1)
    broker2 = Broker(host=host_ips[1][0], port=host_ips[1][1], peers=[host_ips[0]])
    brokers.append(broker2)

    def run_broker(broker: Broker):
        broker.run()

    for broker in brokers:
        threading.Thread(target=run_broker, args=(broker,)).start()
    time.sleep(1)

    # TODO: the cluster has not sharing subscriber list yet, so need to be the same host_ip
    # subscriber
    subscriber = Subscriber(*host_ips[1])
    subscriber.subscribe("topic1")
    threading.Thread(target=subscriber.receive).start()
    time.sleep(1)

    # TODO: the cluster has not sharing subscriber list yet, so need to be the same host_ip
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
