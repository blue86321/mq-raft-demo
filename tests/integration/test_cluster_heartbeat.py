import logging
import time
from typing import List

from src.utils import BROKER_HOST, BROKER_PORT

from src.broker import Broker


def run_cluster_heartbeat(
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
    time.sleep(2)

    for broker in brokers:
        broker.stop()


def test_same_timeout(caplog):
    caplog.set_level(logging.DEBUG)
    run_cluster_heartbeat(same_election_timeout=True)
    assert """[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8001 FOLLOWER] INFO: Running on localhost:8001
""" in caplog.text

    assert """[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1""" in caplog.text
    assert """[Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1""" in caplog.text
    assert """[Broker 8000 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8001""" in caplog.text
    assert """[Broker 8001 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8000""" in caplog.text


def test_random_timeout(caplog):
    caplog.set_level(logging.DEBUG)
    run_cluster_heartbeat(broker_port=BROKER_PORT + 100)

    # Both nodes could be leader
    assert """[Broker 8100 FOLLOWER] INFO: Running on localhost:8100
[Broker 8101 FOLLOWER] INFO: Running on localhost:8101
[Broker 8100 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8100 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8101
[Broker 8101 FOLLOWER] INFO: Vote to leader localhost:8100, term: 1
[Broker 8100 CANDIDATE] DEBUG: Be voted by localhost:8101
[Broker 8100 LEADER] INFO: New leader localhost:8100
[Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8101
[Broker 8101 FOLLOWER] DEBUG: Received heartbeat from localhost:8100, send ACK back
""" in caplog.text or """[Broker 8100 FOLLOWER] INFO: Running on localhost:8100
[Broker 8101 FOLLOWER] INFO: Running on localhost:8101
[Broker 8101 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8101 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8100
[Broker 8100 FOLLOWER] INFO: Vote to leader localhost:8101, term: 1
[Broker 8101 CANDIDATE] DEBUG: Be voted by localhost:8100
[Broker 8101 LEADER] INFO: New leader localhost:8101
[Broker 8101 LEADER] DEBUG: Send heartbeat to peer localhost:8100
[Broker 8100 FOLLOWER] DEBUG: Received heartbeat from localhost:8101, send ACK back
""" in caplog.text
