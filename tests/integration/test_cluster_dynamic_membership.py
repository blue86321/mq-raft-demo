import logging
import time
from typing import List

from src.broker import Broker
from src.utils import BROKER_HOST, BROKER_PORT


def test_cluster_dynamic_membership(caplog):
    caplog.set_level(logging.INFO)
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

    logging.info("\n\n==================== Broker ====================")
    for broker in brokers:
        broker.run()
    time.sleep(2)

    logging.info("\n\n==================== Node Join ====================")

    broker3 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1] + 2,
        join_dest=host_ips[1],
        election_timeout=1,
    )
    broker3.run()
    time.sleep(2)

    logging.info("\n\n==================== Node Leave ====================")

    broker3.stop()
    time.sleep(2)

    # stop
    for broker in brokers:
        broker.stop()

    assert caplog.text == """[root] INFO: 

==================== Broker ====================
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8001 FOLLOWER] INFO: Running on localhost:8001
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1
[Broker 8000 LEADER] INFO: New leader localhost:8000
[root] INFO: 

==================== Node Join ====================
[Broker 8003 FOLLOWER] INFO: Running on localhost:8003
[Broker 8003 FOLLOWER] INFO: Request JOIN_CLUSTER localhost:8001
[Broker 8001 FOLLOWER] INFO: Forward JOIN_CLUSTER to leader: ('localhost', 8000)
[Broker 8000 LEADER] INFO: Sync data with peer localhost:8003
[Broker 8000 LEADER] INFO: New node joins the cluster localhost:8003
[root] INFO: 

==================== Node Leave ====================
[Broker 8000 LEADER] INFO: Node leave the cluster: localhost:8003
"""
