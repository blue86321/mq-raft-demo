import logging
import time
from threading import Thread

from broker import Broker
from subscriber import Subscriber
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT

# Define the topic and message to be published
topic = "topic1"
message = "Hello, world!"
NUM_SUBSCRIBERS=3
# Variable to store the delivery times for each subscriber
delivery_times = []

def start_broker(host, port, peers=None, join_dest=None):
    broker = Broker(host=host, port=port, peers=peers, join_dest=join_dest)
    broker.run()


def measure_delivery_time():
    # Create a publisher and publish the message
    publisher = Publisher()
    publisher.publish(topic, message)

    # Create subscribers and measure the time it takes for them to receive the message
    subscriber_threads = []
    for i in range(NUM_SUBSCRIBERS):
        subscriber = Subscriber()
        thread = Thread(target=subscriber.run)
        subscriber.subscribe(topic)
        subscriber_threads.append(thread)

    # Start the subscriber threads
    start_time = time.time()
    for thread in subscriber_threads:
        thread.start()

    # Wait for all subscriber threads to complete
    for thread in subscriber_threads:
        thread.join()

    # Calculate the delivery time
    end_time = time.time()
    delivery_time = end_time - start_time
    delivery_times.append(delivery_time)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Define the host IPs and ports for the brokers
    host_ips_number=10
    host_ips=[]
    for i in range(host_ips_number):
        host_ips.append((BROKER_HOST,BROKER_PORT+i))
    # host_ips = [(BROKER_HOST, BROKER_PORT), (BROKER_HOST, BROKER_PORT + 1),(BROKER_HOST, BROKER_PORT + 2),(BROKER_HOST, BROKER_PORT + 3),(BROKER_HOST, BROKER_PORT + 4),(BROKER_HOST, BROKER_PORT+10), (BROKER_HOST, BROKER_PORT + 11),(BROKER_HOST, BROKER_PORT + 12),(BROKER_HOST, BROKER_PORT + 13),(BROKER_HOST, BROKER_PORT + 14)]

    # Start the brokers in separate threads
    broker_threads = []
    stop_flag = False  # Flag to indicate when to stop the threads

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


    measure_delivery_time()

    # Print the delivery times
    for i, time in enumerate(delivery_times):
        print(f"Delivery time for Subscriber {i+1}: {time} seconds")

    