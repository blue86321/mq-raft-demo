import logging
import time
from threading import Thread

from broker import Broker
from subscriber import Subscriber
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT


def start_broker(host, port, peers=None, join_dest=None):
    broker = Broker(host=host, port=port, peers=peers, join_dest=join_dest)
    broker.run()
    global stop_threads
    if stop_threads:
        broker.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Define the host IPs and ports for the brokers
    host_ips = [(BROKER_HOST, BROKER_PORT+15), (BROKER_HOST, BROKER_PORT + 16), (BROKER_HOST, BROKER_PORT + 17)]

    # Start the brokers in separate threads
    broker_threads = []
    stop_threads=False
    for i, (host, port) in enumerate(host_ips):
        if i == 0:
            # First broker joins the cluster without specifying peers
            thread = Thread(target=start_broker, args=(host, port))
        else:
            # Other brokers join the cluster by specifying the previous broker as the peer
            thread = Thread(
                target=start_broker,
                args=(host, port),
                kwargs={"peers": [host_ips[i - 1]]},
            )
        thread.start()
        broker_threads.append(thread)

    time.sleep(2)

    topic = "topic1"

    print("\n\n==================== Publish ====================")
    publisher = Publisher()
    publisher.publish(topic, "Hello, world!")
    time.sleep(0.5)

    print("\n\n==================== Subscribe ====================")
    # subscriber
    subscriber = Subscriber()
    subscriber.run()
    subscriber.subscribe(topic)
    time.sleep(1)

    print("\n\n==================== Node D added ====================")
    broker4 = Broker(
        host=host_ips[0][0],
        port=host_ips[0][1] + 3,
        peers=[host_ips[0], host_ips[1], host_ips[2]],
        election_timeout=0.5,
    )
    broker4.run()
    time.sleep(5)
    print("\n\n==================== Node D encounter a problem ====================")
    broker4.stop()
    stop_threads=True

    for broker_thread in broker_threads:
        broker_thread.join()
