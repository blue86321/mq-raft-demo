import logging
import time
from typing import List

from broker import Broker
from utils import BROKER_HOST, BROKER_PORT

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main(same_election_timeout=False):
    # broker
    host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1)]
    brokers: List[Broker] = []

    election_timeout = 0
    if same_election_timeout:
        election_timeout = 0.5

    broker1 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1],
        peers=[host_ips[1]],
        election_timeout=election_timeout,
    )
    brokers.append(broker1)
    broker2 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1],
        peers=[host_ips[0]],
        election_timeout=election_timeout,
    )
    brokers.append(broker2)

    for broker in brokers:
        broker.run()
    time.sleep(2)

    for broker in brokers:
        broker.stop()


if __name__ == "__main__":
    # main()
    main(same_election_timeout=True)
