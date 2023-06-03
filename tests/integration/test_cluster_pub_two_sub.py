import logging
import time
from typing import List

from src.broker import Broker
from src.publisher import Publisher
from src.utils import BROKER_HOST, BROKER_PORT, SUBSCRIBER_PORT
from src.subscriber import Subscriber


def test_cluster_pub_two_pub(caplog):
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
    time.sleep(2.5)

    topic = "topic1"
    # subscriber
    subscribers = [
        Subscriber(*host_ips[0]),
        Subscriber(*host_ips[1], port=SUBSCRIBER_PORT + 5),
    ]
    for idx, sub in enumerate(subscribers):
        logging.info(f"\n\n==================== Subscribe to Node {idx + 1} ====================")
        sub.run()
        sub.subscribe(topic)
    time.sleep(1)

    logging.info("\n\n==================== Publish to Node 2 ====================")
    # publisher
    publisher = Publisher(*host_ips[1])
    publisher.publish(topic, "Hello, world!")
    time.sleep(1)

    logging.info("\n\n==================== Unsubscribe and Publish ====================")
    # unsubscribe
    subscribers[0].unsubscribe(topic)
    logging.info("\n\n==================== Publish (Temporary inconsistent) ====================")
    publisher.publish(topic, "Hello, too fast")
    time.sleep(0.5)
    logging.info("\n\n==================== Publish Later ====================")
    publisher.publish(topic, "Hello, later")
    time.sleep(1)

    # stop
    for sub in subscribers:
        sub.stop()
    for broker in brokers:
        broker.stop()

    assert """==================== Subscribe to Node 1 ====================
[Subscriber 9000] INFO: Subscriber is running on localhost:9000
[Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
""" in caplog.text

    assert """
[Subscriber 9005] INFO: Subscriber is running on localhost:9005
[Subscriber 9005] INFO: SUBSCRIBE message on topic `topic1` at localhost:8001
""" in caplog.text

    # Both receive a message
    assert """==================== Publish to Node 2 ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8001
[Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, world!`
[Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
[Subscriber 9005] INFO: Received message: `Hello, world!` on topic `topic1`
""" in caplog.text or """==================== Publish to Node 2 ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8001
[Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, world!`
[Subscriber 9005] INFO: Received message: `Hello, world!` on topic `topic1`
[Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
""" in caplog.text

    # Now 9000 unsubscribe
    assert """==================== Unsubscribe and Publish ====================
[Subscriber 9000] INFO: UNSUBSCRIBE message on topic `topic1` at localhost:8000
""" in caplog.text

    # Temporary inconsistent
    assert """[Subscriber 9000] INFO: Received message: `Hello, too fast` on topic `topic1`""" in caplog.text
    assert """[Subscriber 9005] INFO: Received message: `Hello, too fast` on topic `topic1`""" in caplog.text

    # Consistent
    assert """==================== Publish Later ====================
[Publisher] INFO: Publish to topic `topic1`: `Hello, later` at localhost:8001
[Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, later`
[Subscriber 9005] INFO: Received message: `Hello, later` on topic `topic1`
""" in caplog.text

    # Later msg will not be received by 9000
    assert """[Subscriber 9000] INFO: Received message: `Hello, later` on topic `topic1`
""" not in caplog.text
