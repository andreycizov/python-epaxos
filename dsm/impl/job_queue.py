Lock
Create
Done
# jjj
# Efficient dependency graph creation.


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

