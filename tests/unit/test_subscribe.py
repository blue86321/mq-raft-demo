import time

from src.broker import Broker
from src.subscriber import Subscriber


def test_subscribe():
    # broker
    broker = Broker()
    broker.run()
    time.sleep(0.5)

    # subscriber
    topic = "topic1"
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(0.5)

    # stop
    broker.stop()
    subscriber.stop()

    assert (subscriber.host, subscriber.port) in broker.topic_subscribers.get(topic)


def test_unsubscribe():
    # broker
    broker = Broker()
    broker.run()
    time.sleep(0.5)

    # subscriber
    topic = "topic1"
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(0.5)

    try:
        assert (subscriber.host, subscriber.port) in broker.topic_subscribers.get(topic)

        subscriber.unsubscribe(topic)
        time.sleep(0.5)
        assert (subscriber.host, subscriber.port) not in broker.topic_subscribers.get(topic)
    finally:
        # stop
        broker.stop()
        subscriber.stop()
