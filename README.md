# Message Broker based on Raft Demo

## Introduction
This is an implementation demo of a distributed message broker based on the [Raft consensus algorithm](https://raft.github.io/).

This message broker is topic-based and distributed. Publishers can send messages on a specific topic, while subscribers can subscribe to multiple topics.


## Features

In this demo, the following mechanisms are implemented: leader election, log replication, and dynamic membership.

### Leader Election
- The leader needs to send heartbeats to all followers.
- Heartbeat messages carry information about all nodes in the cluster, allowing each node to have a complete overview of the cluster.
- Each node has a random timeout, and if it does not receive a heartbeat within that timeout, it becomes a candidate and votes for itself.
- Candidates send a `REQUEST_TO_VOTE` message, asking other nodes to vote for them. Each node can only vote once in a given election term.
- the candidate becomes the new leader when it receives the majority of votes.

### Log Replication
- When the cluster manager receive an update request (Subscribe/Unsubscribe), it forward the request to the leader.
- The leader sends `append entries` messages to all followers along with heartbeats.
- When the majority of followers acknowledge (`ACK`) the `append entries` message, the leader appends the entries locally and sends `LEADER_COMMIT` to all followers.
- Followers update their local entries upon receiving `LEADER_COMMIT` from the leader.

Note: The idea is similar to two-phase commit (2PC) protocol.

### Dynamic Membership
#### Join
- When a new node wants to join, it sends a `JOIN_CLUSTER` message to the cluster manager.
- The `JOIN_CLUSTER` message is forwarded to the leader. The leader then sends a `SYNC_DATA` message to the new node, allowing the new node to obtain all necessary data (e.g., subscribed topics).
- After updating its data, the new node sends an `ACK` to the leader and starts the election timeout mechanism.
- The leader adds the new node to the cluster information and send the information to all followers with each heartbeat.

#### Leave
- The leader sends heartbeats to followers, and if a follower refuses the connection or timeout, it indicates that the node has left the cluster.
- The leader updates the cluster information and sends it to all followers with each heartbeat.

## Run

### Enter Virtual Environment
```shell
python -m venv venv
source venv/bin/activate
pip install .

# Test

# Run Examples...
```

### Exit Environment
```shell
deactivate
rm -rf venv build mq_raft_demo.egg-info .pytest_cache tests/.pytest_cache
```


## Test
```shell
pytest
```

#### Expected Test Result
```
=========== test session starts ============
configfile: pytest.ini
collected 13 items

tests/integration/test_basic.py .                               [  7%]
tests/integration/test_cluster_dynamic_membership_pub_sub.py .  [ 15%]
tests/integration/test_cluster_pub_one_sub.py .                 [ 23%]
tests/integration/test_cluster_pub_two_sub.py .                 [ 30%]
tests/unit/test_borker.py ......                                [ 76%]
tests/unit/test_publish.py .                                    [ 84%]
tests/unit/test_subscribe.py ..                                 [100%]
=========== 13 passed in 38.78s ============
```


## Examples

### Basic (Single Node)
```shell
python examples/basic_single_node.py
```

<img width="700" src="./imgs/DemoBasic.jpg">


```log
==================== Broker ====================
2023-06-07 16:15:41 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 16:15:41 [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
2023-06-07 16:15:42 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:15:42 [Broker 8000 LEADER] INFO: New leader localhost:8000


==================== Subscribe ====================
2023-06-07 16:15:42 [Subscriber 9000] INFO: Subscriber is running on localhost:9000
2023-06-07 16:15:42 [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8888
2023-06-07 16:15:42 [ClusterManager 8888] INFO: Forward SUBSCRIBE to ('localhost', 8000)
2023-06-07 16:15:42 [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
2023-06-07 16:15:42 [Broker 8000 LEADER] INFO: Majority ACK, append entries
2023-06-07 16:15:42 [Broker 8000 LEADER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
2023-06-07 16:15:42 [Subscriber 9000] INFO: Received ACK: subscribe on topic `topic1`


==================== Publish ====================
2023-06-07 16:15:43 [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8888
2023-06-07 16:15:43 [ClusterManager 8888] INFO: Forward PUBLISH to ('localhost', 8000)
2023-06-07 16:15:43 [Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
2023-06-07 16:15:43 [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`
```

### Broker Cluster

#### Leader Election
```shell
python examples/leader_election.py
```

<img width="450" src="./imgs/DemoLeaderElection.jpg">


```log
==================== Same Election Timeout ====================
2023-06-07 16:16:08 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 16:16:08 [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
2023-06-07 16:16:08 [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
2023-06-07 16:16:08 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:16:08 [Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:16:08 [Broker 8000 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8001
2023-06-07 16:16:08 [Broker 8001 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8000
2023-06-07 16:16:09 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 2
2023-06-07 16:16:09 [Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 2
2023-06-07 16:16:09 [Broker 8000 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8001
2023-06-07 16:16:09 [Broker 8001 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8000



==================== Different Election Timeout ====================
2023-06-07 16:16:10 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 16:16:10 [Broker 8100 FOLLOWER] INFO: Running on localhost:8100
2023-06-07 16:16:10 [Broker 8101 FOLLOWER] INFO: Running on localhost:8101
2023-06-07 16:16:10 [Broker 8100 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:16:10 [Broker 8100 CANDIDATE] DEBUG: Send REQUEST_TO_VOTE to localhost:8101
2023-06-07 16:16:10 [Broker 8101 FOLLOWER] INFO: Vote to leader localhost:8100, term: 1
2023-06-07 16:16:10 [Broker 8100 CANDIDATE] DEBUG: Be voted by localhost:8101
2023-06-07 16:16:10 [Broker 8100 LEADER] INFO: New leader localhost:8100
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8888
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8101
2023-06-07 16:16:10 [Broker 8101 FOLLOWER] DEBUG: Received heartbeat from localhost:8100, send ACK back
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8101
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8888
2023-06-07 16:16:10 [Broker 8101 FOLLOWER] DEBUG: Received heartbeat from localhost:8100, send ACK back
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8101
2023-06-07 16:16:10 [Broker 8100 LEADER] DEBUG: Send heartbeat to peer localhost:8888
2023-06-07 16:16:10 [Broker 8101 FOLLOWER] DEBUG: Received heartbeat from localhost:8100, send ACK back
```


#### Fault Tolerance
```shell
python examples/fault_tolerance.py
```

<img width="700" src="./imgs/DemoFaultTolerance.jpg">


```log
==================== Broker ====================
2023-06-07 16:23:45 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 16:23:45 [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
2023-06-07 16:23:45 [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
2023-06-07 16:23:45 [Broker 8002 FOLLOWER] INFO: Running on localhost:8002
2023-06-07 16:23:46 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:23:46 [Broker 8002 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1
2023-06-07 16:23:46 [Broker 8000 LEADER] INFO: New leader localhost:8000
2023-06-07 16:23:46 [Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1


==================== Leader Fail ====================
2023-06-07 16:23:47 [Broker 8001 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 2
2023-06-07 16:23:47 [Broker 8001 CANDIDATE] INFO: Node leave the cluster: localhost:8000
2023-06-07 16:23:47 [Broker 8002 FOLLOWER] INFO: Vote to leader localhost:8001, term: 2
2023-06-07 16:23:47 [Broker 8001 LEADER] INFO: New leader localhost:8001


==================== Another Leader Fail ====================
2023-06-07 16:23:49 [Broker 8002 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 3
2023-06-07 16:23:49 [Broker 8002 CANDIDATE] INFO: Node leave the cluster: localhost:8001
2023-06-07 16:23:49 [Broker 8002 LEADER] INFO: New leader localhost:8002

```


#### Log Replication

##### One Publisher One Subscriber
```shell
python examples/log_replication_pub_sub.py
```

<img width="800" src="./imgs/DemoLogReplication.jpg">

```log
==================== Broker ====================
2023-06-07 16:29:48 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 16:29:48 [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
2023-06-07 16:29:48 [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
2023-06-07 16:29:49 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 16:29:49 [Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1
2023-06-07 16:29:49 [Broker 8000 LEADER] INFO: New leader localhost:8000


==================== Subscribe ====================
2023-06-07 16:29:51 [Subscriber 9000] INFO: Subscriber is running on localhost:9000
2023-06-07 16:29:51 [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8000
2023-06-07 16:29:51 [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
2023-06-07 16:29:51 [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
2023-06-07 16:29:51 [Broker 8000 LEADER] INFO: Majority ACK, append entries
2023-06-07 16:29:51 [Broker 8000 LEADER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
2023-06-07 16:29:51 [Subscriber 9000] INFO: Received ACK: subscribe on topic `topic1`
2023-06-07 16:29:51 [Broker 8001 FOLLOWER] INFO: Received LEADER_COMMIT, persist data
2023-06-07 16:29:51 [Broker 8001 FOLLOWER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000


==================== Publish ====================
2023-06-07 16:29:52 [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8001
2023-06-07 16:29:52 [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, world!`
2023-06-07 16:29:52 [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`


==================== Unsubscribe and Publish ====================
2023-06-07 16:29:53 [Subscriber 9000] INFO: UNSUBSCRIBE message on topic `topic1` at localhost:8000


==================== Publish (Temporary inconsistent) ====================
2023-06-07 16:29:53 [Publisher] INFO: Publish to topic `topic1`: `Hello, too fast` at localhost:8001
2023-06-07 16:29:53 [Broker 8000 LEADER] INFO: New UNSUBSCRIBE to `topic1` from localhost:9000
2023-06-07 16:29:53 [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, too fast`
2023-06-07 16:29:53 [Subscriber 9000] INFO: Received message: `Hello, too fast` on topic `topic1`
2023-06-07 16:29:53 [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
2023-06-07 16:29:53 [Broker 8000 LEADER] INFO: Majority ACK, append entries
2023-06-07 16:29:53 [Broker 8000 LEADER] INFO: Handle append_entries: UNSUBSCRIBE on topic: `topic1` from localhost:9000
2023-06-07 16:29:53 [Subscriber 9000] INFO: Received ACK: unsubscribe on topic `topic1`
2023-06-07 16:29:53 [Broker 8001 FOLLOWER] INFO: Received LEADER_COMMIT, persist data
2023-06-07 16:29:53 [Broker 8001 FOLLOWER] INFO: Handle append_entries: UNSUBSCRIBE on topic: `topic1` from localhost:9000


==================== Publish Later ====================
2023-06-07 16:29:53 [Publisher] INFO: Publish to topic `topic1`: `Hello, later` at localhost:8001
2023-06-07 16:29:53 [Broker 8001 FOLLOWER] INFO: New publish `topic1`: `Hello, later`
```

#### Dynamic Membership
```shell
python examples/dynamic_membership_pub_sub.py
```

<img width="700" src="./imgs/DemoDynamicMembership.jpg">

```log
==================== Broker ====================
2023-06-07 17:17:50 [ClusterManager 8888] INFO: Running on localhost:8888
2023-06-07 17:17:50 [Broker 8000 FOLLOWER] INFO: Running on localhost:8000
2023-06-07 17:17:50 [Broker 8001 FOLLOWER] INFO: Running on localhost:8001
2023-06-07 17:17:50 [Broker 8000 CANDIDATE] INFO: Timeout, sending REQUEST_TO_VOTE, term: 1
2023-06-07 17:17:50 [Broker 8001 FOLLOWER] INFO: Vote to leader localhost:8000, term: 1
2023-06-07 17:17:50 [Broker 8000 LEADER] INFO: New leader localhost:8000


==================== Subscribe ====================
2023-06-07 17:17:52 [Subscriber 9000] INFO: Subscriber is running on localhost:9000
2023-06-07 17:17:52 [Subscriber 9000] INFO: SUBSCRIBE message on topic `topic1` at localhost:8888
2023-06-07 17:17:52 [ClusterManager 8888] INFO: Forward SUBSCRIBE to ('localhost', 8000)
2023-06-07 17:17:52 [Broker 8000 LEADER] INFO: New SUBSCRIBE to `topic1` from localhost:9000
2023-06-07 17:17:52 [Broker 8001 FOLLOWER] INFO: Received append_entries, store in buffer
2023-06-07 17:17:52 [Broker 8000 LEADER] INFO: Majority ACK, append entries
2023-06-07 17:17:52 [Broker 8000 LEADER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000
2023-06-07 17:17:52 [Subscriber 9000] INFO: Received ACK: subscribe on topic `topic1`
2023-06-07 17:17:52 [Broker 8001 FOLLOWER] INFO: Received LEADER_COMMIT, persist data
2023-06-07 17:17:52 [Broker 8001 FOLLOWER] INFO: Handle append_entries: SUBSCRIBE on topic: `topic1` from localhost:9000


==================== Node Join ====================
2023-06-07 17:17:53 [Broker 8006 FOLLOWER] INFO: Running on localhost:8006
2023-06-07 17:17:53 [Broker 8006 FOLLOWER] INFO: Request JOIN_CLUSTER localhost:8888
2023-06-07 17:17:53 [ClusterManager 8888] INFO: Forward JOIN_CLUSTER to ('localhost', 8000)
2023-06-07 17:17:53 [Broker 8000 LEADER] INFO: Sync data with peer localhost:8006
2023-06-07 17:17:53 [Broker 8000 LEADER] INFO: New node joins the cluster localhost:8006


==================== Publish ====================
2023-06-07 17:17:55 [Publisher] INFO: Publish to topic `topic1`: `Hello, world!` at localhost:8888
2023-06-07 17:17:55 [ClusterManager 8888] INFO: Forward PUBLISH to ('localhost', 8000)
2023-06-07 17:17:55 [Broker 8000 LEADER] INFO: New publish `topic1`: `Hello, world!`
2023-06-07 17:17:55 [Subscriber 9000] INFO: Received message: `Hello, world!` on topic `topic1`


==================== Node Leave ====================
2023-06-07 17:17:56 [Broker 8000 LEADER] INFO: Node leave the cluster: localhost:8006

```


## Limitations
### Cluster Node Temporarily Unavailable
- If a node is temporarily unavailable and gets kicked from the cluster, there is no mechanism to gracefully re-join the cluster.

### Message Cache Mechanism
- The broker did not implement a message cache mechanism.
- If a subscriber is unavailable when a message is published, the subscriber will lose the message.
- Implementing a message cache mechanism in a cluster is not easy. The challenge lies in notifying all nodes about the cached messages.
  - When a publish message fails to deliver, we need to forward the failed message to the leader and then notify all nodes to update.
  - When a cached message is re-published, we need to delete the cached message and let the leader notify all nodes to update.

### Sharding
- The system did not implement sharding in the cluster.
- Sharding will partition data into groups and each group will only hold some groups instead of all.
- Sharding can alleviate space limitation and make the system more scalable.

<img width="800" src="./imgs/Sharding.jpg">


### Dedicated Master Node
- The system did not implement dedicated master node.
- In our system, leader will be a bottleneck since it handles not only heartbeats but also requests like publish, subscribe, and unsubscribe.
- A dedicated master node can alleviate this traffic, which only coordinates cluster nodes and does not handle application requests or store data.
- This approach can alleviate the traffic and resolve the leader bottleneck issues.
- However, the dedicated master node will suffer from single-point-of-failure, so we need a cluster for master nodes.

<img width="800" src="./imgs/DedicatedMasterNode.jpg">
