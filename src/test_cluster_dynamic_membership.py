import logging
import time
from typing import List

from broker import Broker
from utils import BROKER_HOST, BROKER_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
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

    print("\n\n==================== Broker ====================")
    for broker in brokers:
        broker.run()
    time.sleep(2)

    
    def process_command(self, command):
        parts = command.split()
        if len(parts) < 3:
            raise ValueError("Invalid command. Usage: add <cluster_id> <node_id>")

        if parts[0].lower() == "add":
            print("\n\n==================== Node Join ====================")
            cluster_id = parts[1]
            node_id = parts[2]
            new_broker = Broker(
                host=cluster_id,
                port=node_id,
                join_dest=host_ips[1],
                election_timeout=1,
            )
            brokers.append(new_broker)
            new_broker.run()
            time.sleep(2)
            # self.add_node_to_cluster(cluster_id, node_id)
            # print(f"Node '{node_id}' added to cluster '{cluster_id}'.")
        elif parts[0].lower() == "del":
            print("\n\n==================== Node Leave ====================")
            broker1.stop()
            time.sleep(2)
        elif parts[0].lower() == "stopall":
            for broker in brokers:
                broker.stop()

    while True:
        command = input("Enter command: ")
        try:
            process_command(command)
        except ValueError as e:
            print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
