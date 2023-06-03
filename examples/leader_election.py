import logging
import time
from typing import List

from src.broker import Broker
from src.utils import BROKER_HOST, BROKER_PORT

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run(
    same_election_timeout=False,
    broker_host: str = BROKER_HOST,
    broker_port: int = BROKER_PORT,
):
    # broker
    host_ips = [(broker_host, broker_port), (broker_host, broker_port + 1)]
    brokers: List[Broker] = []

    election_timeout = 0
    if same_election_timeout:
        election_timeout = 0.8

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
    
    if same_election_timeout:
        time.sleep(2)
    else:
        time.sleep(1)

    for broker in brokers:
        broker.stop()


def main():
    print("\n\n==================== Same Election Timeout ====================")
    run(same_election_timeout=True)
    print("\n\n==================== Different Election Timeout ====================")
    run(broker_port=BROKER_PORT + 100)


if __name__ == "__main__":
    main()
