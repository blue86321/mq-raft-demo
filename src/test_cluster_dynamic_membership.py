import logging
import time
from typing import List

from broker import Broker
from utils import BROKER_HOST, BROKER_PORT

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
    time.sleep(2)

    print("\n\n==================== Node Join ====================")

    broker3 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1] + 2,
        join_dest=host_ips[1],
        election_timeout=1,
    )
    broker3.run()
    time.sleep(2)

    print("\n\n==================== Node Leave ====================")

    broker3.stop()
    time.sleep(2)

    # stop
    for broker in brokers:
        broker.stop()


if __name__ == "__main__":
    main()
