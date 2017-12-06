2017-12-06 20:54 Quorum member slowness
------
The member which is being late is incapable of participating functionally.

 - cross-replica ticking must be highly similar across replicas (must it?)
 - low timeouts with high CPU usage cause constant timeouts of the instances, therefore causing perpetual PrepareRequest.


2017-12-06 12:00 Quorum size and quorum changes. `tags(quorum)`
------
Before I start considering the quorum state change algorithm, I would like to define what does the quorum guarantee?

 - A quorum of size `2*F+1` guarantees that `F` replicas are allowed to stop responding at any time, and the quorum will
 still be able to agree on the command execution order.
   1. On this matter one of the ways to stop a replica is to just kill it, and therefore it will occupy one of the `F` slots
   that are reserved for failed replicas.
      - `q(a)` if we do this, then `fast_path` will never happen for any of the instances that the replica isn't 
      supposed to support.
      - `a(a)` or, with Thrifty this would introduce an inefficiency where it would occupy one of the packets that's 
      expected to be delivered on the optimistic path
   2. We could also introduce a new replica first, and then kill the replica that is supposed to be stopped.
 - How do we introduce a new replica into the consensus without surrendering any of the `F` guarantees provided by the 
 algorithm?

 - The clause "the joining replica becomes live only after receiving commits for all instances included in the replies to 
 at least `F` replicas", is only directed to a case where we are directly replacing the failed replica with a new one, and
 not the case where a replica is either joining or leaving.
   1. When a replica is joining, the replica sends a `JOIN` message and may start replying to the instance requests right away, 
   since after the `JOIN` is sent, the replicas will update their quorum membership information and therefore will consider it
   a part of their quorum right away. They will only commit if the majority of the new quorum agrees to the instance.
   2. When a replica is leaving,   

2017-12-02 23:38 Execution of commands in parallel `tags(executor)`
------
The algorithm provides the dependency graph and the internal sequencing for each of the commands as an output. This means
that we may use that information to build a graph of parallel execution of tasks, further speeding up the execution.

2017-12-02 19:01 Checkpoints `tags(slots)`
-------
Checkpoints are used in order to keep track of what subset of instances have been committed by all of the members of the quorum.
It is not required for the theoretical foundations of the algorithm, yet a practical implementation needs to take the 
memory limits of a single machine into account, therefore we need a mechanism which would allow us to track what information
we may purge as it's no longer required.

The checkpoints are crucial for:
 - **Quorum health checks**: if we know that a checkpoint had been successfully committed, then a majority of replicas
 must have also committed this checkpoint. If they have not - then they should panic implying a member of the quorum has diverged.
 - **Memory management**: depending on the configuration of the algorithm, we may use the checkpoints to approximate which instances
 we may dispose of (and replicas trying to access these instances should panic)

References:
 - `ref(cmd_id_slot_id)`
 
Issues:
 - `q(a)` A command `C` is being proposed with `TS=A`, and it's chosen path is the fast path for members `a,b,c`, 
 therefore it's directly committed.
 - A quorum member does not know about this command yet, but it's also the member which is responsible for the checkpointing.
 - He issues a checkpoint for `TS=A-1` and announces it.
 - Since `a,b,c` learn about the checkpoint later in time, (by the algorithm) they are required to arrange the 
 checkpoint at a later time than a command `C`.
 - So a `TS` of the checkpoint cannot represent the last timestamp of the operations behind it.
 - But when replying to the CP announce, `a,b,c` will include `C` as one of the dependencies for `CP`, therefore letting
 the CP replica know of it. We then use the dependencies of `CP` in order to know the actual `TS` of the checkpoint.
 
 
 - The opposite: a command with `TS=A-1` may end up being executed after the checkpoint has been executed; since it's up
 to the epaxos' discretion to decide the ordering of the commands that all of the replicas will agree with. Means the 
 `TS` will cause false positives.
 
 
 
2017-12-02 18:04 How does a replica ensure exiting a quorum? `tags(quorum)`  
-------------
The algorithm itself allows for any replica to exit at any time; but for the sake of practical implementation this
must be made explicit through a reconfiguration of the quorum. 

Examples:
 - We now have got a new replica which replaces the old one. 
    + `q(a)` How do we mark the failed replica as failed? This is also crucial in order to recycle it's old ID - we
    must be able to mark it as "unused", or as "it must be forgotten at this point already"
    + `q(b)` How do we make sure that an exit is clean, from a perspective of a single replica? How do we ensure that 
    none of it's current instances have been committed and none are being actively created?

2017-12-02 18:00 CommandID to SlotID `tags(slots) id(cmd_id_slot_id)`
--------------
The newer version of command ID passed to the servers should support telling the leader identity from the command ID.

Example structure:

`TS | Leader Slot Part | Instance Slot Part | Random Nonce`

Client connects to the required leader and sends it the command. That way we always make sure commands end up at the same leader.

Issues:
 - `q(a)` When a client re-sends an instance, which was supposed to be lead by it's pre-generated leader
 , but that leader has not returned a response:
    1) **Leader has failed**: none of the replicas at this point will know about this instance and therefore will be 
    forced to begin an explicit prepare session, instead of a normal startup. This will end up with an empty command 
    which will be successfully returned to the client. We can not initiate the command as a different leader, because 
    epaxos makes specific assumptions about the original command leader.
    2) **Leader hasn't failed**: a replica starting an explicit prepare will be forced to agree with the leader's 
    decision at which point it will be able to answer to the client.
 - `q(b)` In such a case, we may also allow for checkpoints `ref(checkpoint)` to also represent the last timestamp at
 which the checkpoint had been committed.
   + Checkpoint sets all of it's deps to "lower than `TS` in the `CommandID` struct".
   + Checkpoint assumes none of the new commands will be accepted that are younger than this checkpoint.
   
   
Guarantees:
 - `g(a)` We tie the checkpoints to real timestamps that clients may use to tell if they're on their HW
 - `g(b)` Clients now will know if their command is too old if they try to re-send it due to them never hearing back 
 from the server. This ensures that clients know for sure that the state of their command is undetermined.
 - `g(c)` This ensures that we don't need to keep track of which commands have been executed post-consensus layer - 
 since their IDs are tied to instances.
 - `g(d)` Noops are now tied to command IDs specified by the clients. Clients know if their command had failed to execute.
 

2017-12-02 18:00 Quorum size and status changes. `tags(quorum)`
- 
A few questions remain unanswered:
 - `q(a)` How does a quorum of size 5 with 2 failed replicas grow in size without breaking the algorithm
   that; if grown by a single replica - will fail to reach an agreement on non-committed instances, since the size of 
   the quorum had increased, but the amount of live replicas hasn't yet?
   - `a(a)` We may first downgrade the size of the quorum to 3 replicas, and then add the new (joining) replica?
   - `q(b)` Would that preserve the guarantees proposed by the initial paper?
