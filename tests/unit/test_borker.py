import logging
import time
from typing import List

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.subscriber import Subscriber

from src.utils import BROKER_HOST, BROKER_PORT


def test_single_node():
    cluster = ClusterManager()
    cluster.run()

    broker = Broker()
    broker.run()
    time.sleep(0.5)
    # stop
    broker.stop()
    cluster.stop()

    assert broker.is_leader

def test_leader_election():
    cluster = ClusterManager()
    cluster.run()

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

    try:
        assert broker1.is_leader
        assert not broker2.is_leader
    finally:
        for broker in brokers:
            broker.stop()
        cluster.stop()


def test_forward_leader():
    cluster = ClusterManager()
    cluster.run()

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

    subscriber = Subscriber()
    try:
        assert broker1.is_leader
        assert not broker2.is_leader

        # subscriber
        topic = "topic1"
        subscriber.run()
        subscriber.subscribe(topic)
        time.sleep(1)

        assert (subscriber.host, subscriber.port) in broker1.topic_subscribers.get(topic)
    finally:
        for broker in brokers:
            broker.stop()
        subscriber.stop()
        cluster.stop()


def test_fault_tolerance():
    # broker
    cluster = ClusterManager()
    cluster.run()

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

    try:
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

        assert broker3.is_leader
        broker3.stop()
    finally:
        broker1.stop()
        broker2.stop()
        broker3.stop()
        cluster.stop()


def test_log_replication():
    cluster = ClusterManager()
    cluster.run()

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
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(0.5)

    try:
        assert broker1.is_leader
        assert not broker2.is_leader

        assert (subscriber.host, subscriber.port) in broker1.sync_data.get(topic)
        assert broker1.sync_data == broker2.sync_data
    finally:
        # stop
        for broker in brokers:
            broker.stop()
        subscriber.stop()
        cluster.stop()


def test_dynamic_membership():
    cluster = ClusterManager()
    cluster.run()

    host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1)]

    broker1 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1],
    )
    broker1.run()
    time.sleep(1)

    # subscriber
    topic = "topic1"
    subscriber = Subscriber()
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
    time.sleep(1.5)

    try:
        assert broker1.is_leader
        assert (broker2.host, broker2.port) in broker1.peers
        assert (broker1.host, broker1.port) in broker2.peers

        assert (subscriber.host, subscriber.port) in broker1.sync_data.get(topic)
        assert broker1.sync_data == broker2.sync_data

        # node leave
        broker2.stop()
        time.sleep(0.5)
        assert (broker2.host, broker2.port) not in broker1.peers
    finally:
        # stop
        broker2.stop()
        broker1.stop()
        subscriber.stop()
        cluster.stop()
