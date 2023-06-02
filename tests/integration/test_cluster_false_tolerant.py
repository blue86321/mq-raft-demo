import logging
import time
from typing import List

from src.broker import Broker
from src.utils import BROKER_HOST, BROKER_PORT


def test_cluster_false_tolerant(caplog):
    caplog.set_level(logging.INFO)
    # broker
    host_ips = [
        (BROKER_HOST, BROKER_PORT),
        (BROKER_HOST, BROKER_PORT + 1),
        (BROKER_HOST, BROKER_PORT + 2),
    ]
    brokers: List[Broker] = []

    broker1 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1],
        peers=[host_ips[1], host_ips[2]],
        election_timeout=0.5,
    )
    brokers.append(broker1)
    broker2 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1],
        peers=[host_ips[0], host_ips[2]],
        election_timeout=1,
    )
    brokers.append(broker2)
    broker3 = Broker(
        host=host_ips[2][0],
        port=host_ips[2][1],
        peers=[host_ips[0], host_ips[1]],
        election_timeout=2,
    )
    brokers.append(broker3)

    logging.info("\n\n==================== Broker ====================")
    for broker in brokers:
        broker.run()
    time.sleep(2)

    logging.info("\n\n==================== Leader Fail ====================")
    broker1.stop()
    time.sleep(2)

    # stop
    for broker in brokers:
        broker.stop()

    assert """[root] INFO: 

==================== Broker ====================
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8001 FOLLOWER] INFO: Running on localhost:8001
[Broker 8002 FOLLOWER] INFO: Running on localhost:8002
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
""" in caplog.text

    # FOLLOWER vote order can be different
    assert """[Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1""" in caplog.text
    assert """[Broker 8002 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1""" in caplog.text
    assert """[Broker 8000 LEADER] INFO: New leader localhost:8000""" in caplog.text

    assert """
==================== Leader Fail ====================
[Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 2
[Broker 8001 CANDIDATE] INFO: Node leave the cluster: localhost:8000
[Broker 8002 FOLLOWER] INFO: Vote to leader localhost:8001, term: 2
[Broker 8001 LEADER] INFO: New leader localhost:8001
""" in caplog.text or """
==================== Leader Fail ====================
[Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 2
[Broker 8002 FOLLOWER] INFO: Vote to leader localhost:8001, term: 2
[Broker 8001 CANDIDATE] INFO: Node leave the cluster: localhost:8000
[Broker 8001 LEADER] INFO: New leader localhost:8001
""" in caplog.text
