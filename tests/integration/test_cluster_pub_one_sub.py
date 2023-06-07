import logging
import time
from typing import List

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.publisher import Publisher
from src.utils import BROKER_HOST, BROKER_PORT
from src.subscriber import Subscriber


def test_cluster_pub_one_pub(caplog):
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
    cluster = ClusterManager()
    cluster.run()
    for broker in brokers:
        broker.run()
    time.sleep(2.5)

    topic = "topic1"
    logging.info("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    logging.info("\n\n==================== Publish ====================")
    # publisher
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(1)

    logging.info("\n\n==================== Unsubscribe and Publish ====================")
    # unsubscribe
    subscriber.unsubscribe(topic)
    logging.info("\n\n==================== Publish (Temporary inconsistent) ====================")
    publisher.publish(topic, "Hello, too fast")
    time.sleep(0.5)
    logging.info("\n\n==================== Publish Later ====================")
    publisher.publish(topic, "Hello, later")
    time.sleep(1)

    # stop
    subscriber.stop()
    for broker in brokers:
        broker.stop()
    cluster.stop()

    print(caplog.text)
    assert """[root] INFO: 

==================== Broker ====================
[ClusterManager 8888] INFO: Running on localhost:8888
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8001 FOLLOWER] INFO: Running on localhost:8001
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1
[Broker 8000 LEADER] INFO: New leader localhost:8000
[root] INFO: 

==================== Subscribe ====================
[Subscriber 9000] INFO: Subscriber is running on localhost:9000
[Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8888
[ClusterManager 8888] INFO: Forward SUBSCRIBE to ('localhost', 8000)
[Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
[Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
[Broker 8000 LEADER] INFO: Majority ACK, append entries
[Broker 8000 LEADER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
[Subscriber 9000] INFO: Received ACK: subscribe on topic `topic1`
[Broker 8001 FOLLOWER] INFO: Received LEADER_COMMIT, persist data
[Broker 8001 FOLLOWER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
[root] INFO: 

==================== Publish ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8888
""" in caplog.text

    assert "[Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`" in caplog.text

    # Unsubscribe
    assert "[Subscriber 9000] INFO: UNSUBSCRIBE message on topic `topic1` at localhost:8888" in caplog.text

    # Log replication order
    assert "[Broker 8000 LEADER] INFO: New UNSUBSCRIBE to `topic1` from localhost:9000" in caplog.text
    assert "[Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer" in caplog.text
    assert "[Broker 8000 LEADER] INFO: Majority ACK, append entries" in caplog.text
    assert "[Broker 8000 LEADER] INFO: Handle append_entries: UNSUBSCRIBE on topic: `topic1` from localhost:9000" in caplog.text
    assert "[Broker 8001 FOLLOWER] INFO: Handle append_entries: UNSUBSCRIBE on topic: `topic1` from localhost:9000" in caplog.text

    # Temporary inconsistent
    assert """[Subscriber 9000] INFO: Received message: `Hello, too fast` on topic `topic1`""" in caplog.text
    # later, consistent
    assert """[Subscriber 9000] INFO: Received message: `Hello, later` on topic `topic1`""" not in caplog.text
