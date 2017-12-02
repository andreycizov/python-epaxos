2017-12-31 19:01 Checkpoints `tags(slots)`
-------
References:
 - `ref(cmd_id_slot_id)`

What are we trying to solve?

Issues:

 
Issues:
 - `q(a)` A command `C` is being proposed with `TS=A`, and it's chosen path is the fast path for members `a,b,c`, therefore it's directly committed.
 - A quorum member does not know about this command yet, but it's also the member which is responsible for the checkpointing.
 - He issues a checkpoint for `TS=A-1` and announces it.
 - Since `a,b,c` learn about the checkpoint later in time, (by the algorithm) they are required to arrange the checkpoint at a later time than a command `C`.
 - So a `TS` of the checkpoint cannot represent the last timestamp of the operations behind it.
 - But when replying to the CP announce, `a,b,c` will include `C` as one of the dependencies for `CP`, therefore letting
 the CP replica know of it. We then use the dependencies of `CP` in order to know the actual `TS` of the checkpoint.
 
 
 - The opposite: a command with `TS=A-1` may end up being executed after the checkpoint has been executed; since it's up
 to the epaxos' discretion to decide the ordering of the commands that all of the replicas will agree with. Means the `TS` will cause
 false positives.
 
 - What if we had a fence over several checkpoints, that would give us a direct range of values ?
 
Resolutions:
  - `a(a)` We may ensure that all of the replicas presume the existence of a checkpoint command at any time, which means 
  - `a'(a)` 
 
2017-12-31 18:04 How does a replica ensure exiting a quorum? `tags(quorum)`  
-------------
The algorithm itself allows for any replica to exit at any time; but for the sake of practical implementation this
must be made explicit through a reconfiguration of the quorum. 

Examples:
 - We now have got a new replica which replaces the old one. 
    + `q(a)` How do we mark the failed replica as failed? This is also crucial in order to recycle it's old ID - we
    must be able to mark it as "unused", or as "it must be forgotten at this point already"
    + `q(b)` How do we make sure that an exit is clean, from a perspective of a single replica? How do we ensure that none of it's
    current instances have been committed and none are being actively created?

2017-12-31 18:00 CommandID to SlotID `tags(slots) id(cmd_id_slot_id)`
--------------
The newer version of command ID passed to the servers should support telling the leader identity from the command ID.

Example structure:

`TS | Leader Slot Part | Instance Slot Part | Random Nonce`

Client connects to the required leader and sends it the command. That way we always make sure commands end up at the same leader.

Issues:
 - `q(a)` When a client re-sends an instance, which was supposed to be lead by it's pre-generated leader
 , but that leader has not returned a response:
    1) **Leader has failed**: none of the replicas at this point will know about this instance and therefore will be forced to begin an explicit prepare
 session, instead of a normal startup. This will end up with an empty command which will be successfully returned to the client. We can not initiate the command as
 a different leader, because epaxos makes specific assumptions about the original command leader.
    2) **Leader hasn't failed**: a replica starting an explicit prepare will be forced to agree with the leader's decision at which point it will
    be able to answer to the client.
 - `q(b)` In such a case, we may also allow for checkpoints `ref(checkpoint)` to also represent the last timestamp at
 which the checkpoint had been committed.
   + Checkpoint sets all of it's deps to "lower than `TS` in the `CommandID` struct".
   + Checkpoint assumes none of the new commands will be accepted that are younger than this checkpoint.
   
   
Guarantees:
 - `g(a)` We tie the checkpoints to real timestamps that clients may use to tell if they're on their HW
 - `g(b)` Clients now will know if their command is too old if they try to re-send it due to them never hearing back from the server.
 This ensures that clients know for sure that the state of their command is undetermined.
 - `g(c)` This ensures that we don't need to keep track of which commands have been executed post-consensus layer - since their IDs are tied to instances.
 - `g(d)` Noops are now tied to command IDs specified by the clients. Clients know if their command had failed to execute.
 

2017-12-31 18:00 Quorum size and status changes. `tags(quorum)`
- 
A few questions remain unanswered:
 - `q(a)` How does a quorum of size 5 with 2 failed replicas grow in size without breaking the algorithm
   that; if grown by a single replica - will fail to reach an agreement on non-committed instances, since the size of the quorum had
   increased, but the amount of live replicas hasn't yet?
 -  
