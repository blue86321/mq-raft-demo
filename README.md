# Message Broker based on Raft Demo

## Introduction
This is an implementation demo of a distributed message broker based on [Raft consensus algorithm](https://raft.github.io/).

This message broker is topic-based and distributed.
A publisher needs to send messages on a topic. A subscriber can subscribe to multiple topics.


In this demo, the following mechanisms are implemented: leader election and log replication.

### Raft leader election
- Leader need to send heartbeat to all followers
- All nodes have a random timeout, if they did not receive a heartbeat within that timeout, they become candidates and vote itself
- Candidates send `REQUEST_TO_VOTE` asking others to vote it, every node can only vote once in a given election term
- When the vote exceeds the majority of nodes, the candidate becomes a new leader

### Raft log replication
- Un/subscribe will forward to the leader
- Leader will send `append entries` (un/subscribe) to all followers along with heartbeats
- When the majority of followers `ACK`, the leader will append entries locally
- Followers update their local entries when receiving the next heartbeat after `append entries` message.

## Run

### Virtual Environment
```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Demo
- Basic
  ```shell
  python src/test_basic.py
  ```
  ```log
  [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
  [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
  [Broker 8000 LEADER] INFO: New leader localhost:8000
  [Subscriber 9000] INFO: Subscriber is running on localhost:9000
  [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
  [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8000
  [Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
  [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
  ```

- Cluster with one subscriber
  ```shell
  python src/test_broker_cluster_one_subscriber.py
  ```
  ```log
  [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
  [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
  [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
  [Broker 8000 LEADER] INFO: New leader localhost:8000
  [Subscriber 9000] INFO: Subscriber is running on localhost:9000
  [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
  [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
  [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Broker 8001 FOLLOWER] INFO: Handle append_entries from the leader
  [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8001
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, world!`
  [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
  [Subscriber 9000] INFO: UNSUBSCRIBE message on topic `topic1` at localhost:8000
  [Publisher] INFO: Publish to topic `topic1`: `Hello, too fast` at localhost:8001
  [Broker 8000 LEADER] INFO: New UNSUBSCRIBE to `topic1` from localhost:9000
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, too fast`
  [Subscriber 9000] INFO: Received message: `Hello, too fast` on topic `topic1`
  [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Broker 8001 FOLLOWER] INFO: Handle append_entries from the leader
  [Publisher] INFO: Publish to topic `topic1`: `Hello, later` at localhost:8001
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, later`
  ```

- Cluster with two subscribers
  ```shell
  python src/test_broker_cluster_two_subscribers.py
  ```
  ```log
  [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
  [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
  [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
  [Broker 8000 LEADER] INFO: New leader localhost:8000
  [Subscriber 9000] INFO: Subscriber is running on localhost:9000
  [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
  [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
  [Subscriber 9005] INFO: Subscriber is running on localhost:9005
  [Subscriber 9005] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
  [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9005
  [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Broker 8001 FOLLOWER] INFO: Handle append_entries from the leader
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
  [Broker 8001 FOLLOWER] INFO: Handle append_entries from the leader
  [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8001
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, world!`
  [Subscriber 9005] INFO: Received message: `Hello, world!` on topic `topic1`
  [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
  [Subscriber 9000] INFO: UNSUBSCRIBE message on topic `topic1` at localhost:8000
  [Broker 8000 LEADER] INFO: New UNSUBSCRIBE to `topic1` from localhost:9000
  [Publisher] INFO: Publish to topic `topic1`: `Hello, too fast` at localhost:8001
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, too fast`
  [Subscriber 9005] INFO: Received message: `Hello, too fast` on topic `topic1`
  [Subscriber 9000] INFO: Received message: `Hello, too fast` on topic `topic1`
  [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
  [Broker 8000 LEADER] INFO: Majority ACK, append entries
  [Broker 8001 FOLLOWER] INFO: Handle append_entries from the leader
  [Publisher] INFO: Publish to topic `topic1`: `Hello, later` at localhost:8001
  [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, later`
  [Subscriber 9005] INFO: Received message: `Hello, later` on topic `topic1`
  ```

### Exit Environment
```shell
deactivate
rm -rf venv
```