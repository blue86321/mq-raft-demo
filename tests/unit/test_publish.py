import logging
import time

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.publisher import Publisher


def test_publish(caplog):
    caplog.set_level(logging.INFO)
    # broker
    logging.info("\n\n==================== Broker ====================")
    cluster = ClusterManager()
    cluster.run()
    broker = Broker()
    broker.run()
    time.sleep(1)

    topic = "topic1"

    # publisher
    logging.info("\n\n==================== Publish ====================")
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(0.5)

    # stop
    broker.stop()
    cluster.stop()
    print(caplog.text)
    assert caplog.text == """[root] INFO: 

==================== Broker ====================
[ClusterManager 8888] INFO: Running on localhost:8888
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8000 LEADER] INFO: New leader localhost:8000
[root] INFO: 

==================== Publish ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8888
[ClusterManager 8888] INFO: Forward PUBLISH to ('localhost', 8000)
[Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
"""
