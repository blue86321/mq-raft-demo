import logging
import time

from src.broker import Broker
from src.publisher import Publisher


def test_publish(caplog):
    caplog.set_level(logging.INFO)
    # broker
    logging.info("\n\n==================== Broker ====================")
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
    print(caplog.text)
    assert caplog.text == """[root] INFO: 

==================== Broker ====================
[Broker 8000 FOLLOWER] INFO: Running on localhost:8000
[Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
[Broker 8000 LEADER] INFO: New leader localhost:8000
[root] INFO: 

==================== Publish ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8000
[Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
"""
