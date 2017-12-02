PyDSM - Reference epaxos implementation for Python3.6
==========
Playing with the epaxos before transferring this to a more high-performing language.

### What is done

 - An working agreement protocol as described in the paper
 - Checkpointing
 - Purges of committed instances given that they have been agreed on in the previous version
 - Divergence errors - tell a replica we do not accept commands younger than the last checkpoint

### TODO

Work in progress diary is in [future.md](./docs/future.md).

 - Quorum membership changes
   - Joining of a replica (increase the epoch number, sync the state).
   - Leaving of a replica (by the protocol guarantees that may happen at any time, but we may find a better way to share that).

 - Implement Thrifty version of the protocol and a faster version of paxos.
 - Implement practical extensions as describen in [future.md](./docs/future.md):
   - Better checkpointing by introducing a sliding window of earliest accepted commands 
   - Getting rid of sequential slots as described in the paper and introducing slots that are correlated with request IDs.

### Notes
I am interested in implementing a generalised e-paxos based interface which could then be plugged into any state machine in order to make it distributed. This involves:
  -  Providing a (pluggable) interface to keep track of command dependencies depending on whether the given commands commute.
  -  Providing a (pluggable) interface to notify of commands pending to be  executed after the algorithm has reached consensus.
  -  Providing a (pluggable) interface to stop and start the state machine after failure or during routine maintenance.

### Running

```bash
pip install -r requirements.txt
python3.6 dsm_tests/epaxos/zeromq.py
```
### References

Please note the original author of the algorithm has also published a [Go](https://github.com/efficient/epaxos) version of the algorithm. 

### Notes

This implementation agrees on 1000 requests per second in a configuration of 5 replicas with an average latency of 2.5ms on local network running on MBP 2014.

AUTHOR:

Andrey Cizov (acizov@gmail.com)