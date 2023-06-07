import logging
import time

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.publisher import Publisher
from src.subscriber import Subscriber


def test_basic(caplog):
    caplog.set_level(logging.INFO)
    # broker
    logging.info("\n\n==================== Broker ====================")
    cluster = ClusterManager()
    cluster.run()
    broker = Broker()
    broker.run()
    time.sleep(1)

    topic = "topic1"

    logging.info("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    # publisher
    logging.info("\n\n==================== Publish ====================")
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(0.5)

    # stop
    subscriber.stop()
    broker.stop()
    cluster.stop()

    assert caplog.text == """[root] INFO: 

==================== Broker ====================
[ClusterManager 8888] INFO: Running on localhost:8888
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8000 LEADER] INFO: New leader localhost:8000
[root] INFO: 

==================== Subscribe ====================
[Subscriber 9000] INFO: Subscriber is running on localhost:9000
[Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8888
[ClusterManager 8888] INFO: Forward SUBSCRIBE to ('localhost', 8000)
[Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
[Broker 8000 LEADER] INFO: Majority ACK, append entries
[Broker 8000 LEADER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
[Subscriber 9000] INFO: Received ACK: subscribe on topic `topic1`
[root] INFO: 

==================== Publish ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8888
[ClusterManager 8888] INFO: Forward PUBLISH to ('localhost', 8000)
[Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
[Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
"""
