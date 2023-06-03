import logging
import time
from typing import List

from src.broker import Broker
from src.subscriber import Subscriber

from src.utils import BROKER_HOST, BROKER_PORT


def test_single_node():
    broker = Broker()
    broker.run()
    time.sleep(0.5)
    # stop
    broker.stop()

    assert broker.is_leader


def run_cluster_heartbeat(same_election_timeout=False, broker_host: str = BROKER_HOST, broker_port: int = BROKER_PORT):
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


def test_heartbeat_same_timeout(caplog):
    caplog.set_level(logging.DEBUG)
    run_cluster_heartbeat(same_election_timeout=True)
    assert """[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8001 FOLLOWER] INFO: Running on localhost:8001
""" in caplog.text

    assert """[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1""" in caplog.text
    assert """[Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1""" in caplog.text
    assert """[Broker 8000 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8001""" in caplog.text
    assert """[Broker 8001 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8000""" in caplog.text


def test_heartbeat_random_timeout(caplog):
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


def test_leader_election():
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
        election_timeout=0.8,
    )
    brokers.append(broker2)

    for broker in brokers:
        broker.run()
    time.sleep(1)

    assert broker1.is_leader
    assert not broker2.is_leader

    for broker in brokers:
        broker.stop()


def test_forward_leader():
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
        election_timeout=0.8,
    )
    brokers.append(broker2)

    for broker in brokers:
        broker.run()
    time.sleep(1)

    assert broker1.is_leader
    assert not broker2.is_leader

    # subscriber
    topic = "topic1"
    subscriber = Subscriber(broker_host=broker2.host, broker_port=broker2.port)
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    assert (subscriber.host, subscriber.port) in broker1.topic_subscribers.get(topic)

    for broker in brokers:
        broker.stop()
    subscriber.stop()


def test_fault_tolerance(caplog):
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
        election_timeout=1.5,
    )
    brokers.append(broker3)

    for broker in brokers:
        broker.run()
    time.sleep(1)

    assert broker1.is_leader
    assert not broker2.is_leader
    assert not broker3.is_leader

    # leader fail
    broker1.stop()
    time.sleep(1)

    assert broker2.is_leader
    assert not broker3.is_leader

    # leader fail
    broker2.stop()
    time.sleep(2)

    print(caplog.text)

    assert broker3.is_leader
    broker3.stop()


def test_log_replication():
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
        election_timeout=0.8,
    )
    brokers.append(broker2)

    for broker in brokers:
        broker.run()
    time.sleep(1)

    # subscriber
    topic = "topic1"
    subscriber = Subscriber(broker_host=host_ips[0][0], broker_port=host_ips[0][1])
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(0.5)

    assert broker1.is_leader
    assert not broker2.is_leader

    assert (subscriber.host, subscriber.port) in broker1.sync_data.get(topic)
    assert broker1.sync_data == broker2.sync_data

    # stop
    for broker in brokers:
        broker.stop()
    subscriber.stop()


def test_dynamic_membership():
    host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1)]

    broker1 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1],
    )
    broker1.run()
    time.sleep(0.5)

    # subscriber
    topic = "topic1"
    subscriber = Subscriber(broker_host=host_ips[0][0], broker_port=host_ips[0][1])
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(0.5)

    # new node
    broker2 = Broker(
        host=host_ips[1][0],
        port=host_ips[1][1],
        join_dest=host_ips[0],
    )
    broker2.run()
    time.sleep(1)

    assert broker1.is_leader
    assert (broker2.host, broker2.port) in broker1.peers
    assert (broker1.host, broker1.port) in broker2.peers

    assert (subscriber.host, subscriber.port) in broker1.sync_data.get(topic)
    assert broker1.sync_data == broker2.sync_data

    # node leave
    broker2.stop()
    time.sleep(0.5)
    assert (broker2.host, broker2.port) not in broker1.peers

    # stop
    broker1.stop()
    subscriber.stop()
