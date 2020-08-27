
import time
import unittest

from slurmpy.slurmpy import Slurm, SlurmException

class TestSlurmpy(unittest.TestCase):
    def setUp(self):
      self.slurm_queue_args = {"partition": "hnm", "time": "00:00:15", 'no-requeue': None, "Q": None}

    def test_silent_query_of_nonexistent_job(self):
        ret = Slurm.query('101', on_failure='silent')
        self.assertIsNone(ret)

    def test_exception_query_of_nonexistent_job(self):
        with self.assertRaises(SlurmException):
            Slurm.query('101')

    def test_sending_to_queue(self):
        s = Slurm("job-name", self.slurm_queue_args)
        job_id = s.run('sleep 5')
        self. assertTrue(s.still_running(job_id))

    def test_sending_local(self):
        s = Slurm("job-name")
        job_id = s.run('sleep 5', local=True)
        self.assertTrue(s.still_running(job_id))
        time.sleep(7)
        self. assertFalse(s.still_running(job_id))

    def test_multiple_local_sends(self):
        s = Slurm("job-name")
        ids = []
        for i in range(5):
            ids.append(s.run('sleep 5', local=True))

    def test_multiple_queue_sends(self):
        s = Slurm("job-name")
        ids = []
        for i in range(5):
            ids.append(s.run('sleep 5'))

    def test_kill_local (self):
        s = Slurm("job-name")
        job_id = s.run('sleep 10', local=True)
        self.assertTrue(s.still_running(job_id))
        s.kill(job_id)
        self.assertFalse(s.still_running(job_id))

    def test_kill_queue(self):
        s = Slurm("job-name", self.slurm_queue_args)
        job_id = s.run('sleep 10', local=True)
        self.assertTrue(s.still_running(job_id))
        s.kill(job_id)
        self.assertFalse(s.still_running(job_id))

if __name__ == '__main__':
    unittest.main()
