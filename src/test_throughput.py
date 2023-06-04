import logging
import time
from threading import Thread

from broker import Broker
from subscriber import Subscriber
from publisher import Publisher
from utils import BROKER_HOST, BROKER_PORT


def start_broker(host, port, peers=None, join_dest=None):
    global stop_flag
    broker = Broker(host=host, port=port, peers=peers, join_dest=join_dest)
    broker.run()
    if stop_flag:
        time.sleep(2)
        broker.stop()



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
    broker_threads = []
    for i in range(host_ips_number):
        host_ips.append((BROKER_HOST,BROKER_PORT+i))
        if i == 0:
            # First broker joins the cluster without specifying peers
            thread = Thread(target=start_broker, args=(BROKER_HOST, BROKER_PORT+i))
        else:
            # Other brokers join the cluster by specifying the previous broker as the peer
            thread = Thread(
                target=start_broker,
                args=(BROKER_HOST, BROKER_PORT+i),
                kwargs={"peers": host_ips},
            )
        thread.start()
        broker_threads.append(thread)

    time.sleep(2)

    topic = "topic1"

    print("\n\n==================== Publish ====================")
    publisher = Publisher()

    # Measure throughput for 5 seconds
    measurement_duration = 5
    throughput_thread = Thread(target=measure_throughput, args=(measurement_duration))
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
