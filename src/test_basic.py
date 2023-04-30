import logging
import threading
import time

from broker import Broker
from publisher import Publisher
from subscriber import Subscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    # broker
    broker = Broker()

    def run_broker(broker: Broker):
        broker.run()

    threading.Thread(target=run_broker, args=(broker,)).start()
    time.sleep(1)

    # subscriber
    subscriber = Subscriber()
    subscriber.subscribe("topic1")
    threading.Thread(target=subscriber.receive).start()
    time.sleep(1)

    # publisher
    publisher = Publisher()
    publisher.publish("topic1", "Hello, world!")
    time.sleep(1)

    # stop
    subscriber.stop()
    broker.stop()


if __name__ == "__main__":
    main()
