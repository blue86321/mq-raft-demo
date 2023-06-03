import random
import time
from threading import Thread

from broker import Broker
from subscriber import Subscriber
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT

# Define the topic and message to be published
topic = "topic1"
message = "Hello, world!"

# Number of subscribers
NUM_SUBSCRIBERS = 3

# Define the workload parameters
publish_rate = 2  # Number of publish requests per second
subscribe_rate = 1  # Number of subscribe requests per second
workload_duration = 10  # Duration of the workload in seconds

def start_broker(host, port, peers=None, join_dest=None):
    broker = Broker(host=host, port=port, peers=peers, join_dest=join_dest)
    broker.run()

def publish_workload():
    publisher = Publisher()
    while True:
        publisher.publish(topic, message)
        time.sleep(1 / publish_rate)

def subscribe_workload():
    subscriber_threads = []
    for i in range(NUM_SUBSCRIBERS):
        subscriber = Subscriber()
        thread = Thread(target=subscriber.run)
        subscriber_threads.append(thread)

    for thread in subscriber_threads:
        thread.start()

def generate_workload():
    publish_thread = Thread(target=publish_workload)
    subscribe_thread = Thread(target=subscribe_workload)

    publish_thread.start()
    subscribe_thread.start()

    time.sleep(workload_duration)

    publish_thread.join()
    subscribe_thread.join()

if __name__ == "__main__":
    # Start the broker
    host_ips_number=10
    host_ips=[]
    broker_threads = []
    for i in range(host_ips_number):
        host_ips.append((BROKER_HOST,BROKER_PORT+i))

        for i, (host, port) in enumerate(host_ips):
            if i == 0:
                # First broker joins the cluster without specifying peers
                thread = Thread(target=start_broker, args=(host, port))
            else:
                # Other brokers join the cluster by specifying the previous broker as the peer
                thread = Thread(
                    target=start_broker,
                    args=(host, port),
                    kwargs={"peers": [host_ips[i-1]]},
                )
            thread.start()
            broker_threads.append(thread)

    # Wait for the broker to start
    time.sleep(1)

    # Generate the workload
    generate_workload()

    # # Stop the broker
    # broker.stop()
    # broker_thread.join()
