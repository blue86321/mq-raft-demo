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


def measure_throughput(duration):
    start_time = time.time()
    message_count = 0

    while time.time() - start_time < duration:
        # Generate and publish messages at a high rate
        publisher.publish(topic, "Hello, world!")
        message_count += 1

    elapsed_time = time.time() - start_time
    throughput = message_count / elapsed_time
    print(f"Throughput: {throughput} messages/second")


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

    time.sleep(2)

    topic = "topic1"

    print("\n\n==================== Publish ====================")
    publisher = Publisher()

    # Measure throughput for 5 seconds
    measurement_duration = 5
    throughput_thread = Thread(target=measure_throughput, args=(measurement_duration,))
    throughput_thread.start()

    # Publish messages for the specified duration
    start_time = time.time()
    while time.time() - start_time < measurement_duration:
        publisher.publish(topic, "Hello, world!")
        time.sleep(0.01)  # Adjust the publishing rate as desired

    # Stop the throughput measurement
    throughput_thread.join()

    # Stop the brokers
    stop_flag = True

    # Wait for all broker threads to complete
    for thread in broker_threads:
        thread.join()

    # Perform additional analysis and reporting
