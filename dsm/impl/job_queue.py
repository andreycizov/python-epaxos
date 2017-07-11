# Lock
# Create
# Done
# jjj
# Efficient dependency graph creation.
from datetime import datetime
from typing import NamedTuple, List
from uuid import uuid4

x = {
    1: ('execute_start', 1, 2, 3, 4, 5),
    2: ('execute_poll', 1, 2, 3, 4, 5),
    3: ('execute_shell', None),
}


class JobID(NamedTuple):
    id: uuid4


class LockID(NamedTuple):
    id: uuid4


class CreateJob(NamedTuple):
    id: JobID
    name: str


class LockJobs(NamedTuple):
    lock: LockID
    time: datetime
    jobs: List[JobID]


class PingLock(NamedTuple):
    lock: LockID
    time: datetime


class CompleteJobs(NamedTuple):
    lock: LockID
    completed: List[JobID]
    created: List[CreateJob]


class Failure(NamedTuple):
    pass


class Success(NamedTuple):
    pass


class JobQueue:
    def worker_register(self, worker_id):
        # TODO: this means that a worker needs to register with the whole consensus
        pass

    def worker_ping(self, worker_id):
        # TODO: pass
        pass

    def create(self, job_set):
        pass

    def lock(self, job_id_set, worker_id, timeout):
        pass

    def done(self, lock_id, ):
        pass
