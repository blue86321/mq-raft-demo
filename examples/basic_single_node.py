import logging
import time

from src.broker import Broker
from src.cluster_manager import ClusterManager
from src.publisher import Publisher
from src.subscriber import Subscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    # broker
    print("\n\n==================== Broker ====================")
    cluster = ClusterManager()
    cluster.run()
    broker = Broker()
    broker.run()
    time.sleep(1)

    topic = "topic1"
    
    print("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    # publisher
    print("\n\n==================== Publish ====================")
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(0.5)

    # stop
    subscriber.stop()
    broker.stop()
    cluster.stop()


if __name__ == "__main__":
    main()
