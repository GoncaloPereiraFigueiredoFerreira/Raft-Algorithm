# Raft-Algorithm

An implementation of the Raft algorithm that follows the work of the paper "Leader or Majority: Why have one when you can have both?
Improving Read Scalability in Raft-like consensus protocols" and "CONSENSUS: BRIDGING THEORY AND PRACTICE", with the addition of pre-votting and the disttribution of read queries, between the leader and a quorum of followers.

For testing the group relied mostly on the "maelstrom" testing tool, that allowed the devellopment group to find and fix multiple issues, and assert the functionality of the algorithm in situations of network partitions and variable latencies.

## Maelstrom testing

#1- Basic test with constant latency distribution and a slow rate of request

./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 25 --latency-dist constant --rate 10 

#2- Test with variable latency following an exponential distribuition and a slow rate of requests (will most likely cause leadership changes and pre-vote prevented election situations)

./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 40  --latency-dist exponential --rate 10 

#3- Test that introduces random partitions between nodes (will cause data loss in nodes, and subsequent recovery of them, coupled with leader elections)

./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 25 --latency-dist uniform --rate 10 --nemesis partition --nemesis-interval 10
