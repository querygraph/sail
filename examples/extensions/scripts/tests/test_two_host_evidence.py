"""Worker completion evidence must distinguish work from endpoint readiness."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from two_host import completed_worker_tasks


class WorkerEvidence(unittest.TestCase):
    def test_only_successful_worker_task_records_count(self):
        log = """
[2026-09-26T05:05:16Z INFO sail_execution::driver::actor::handler] worker 3 is available at 192.0.2.12:50162
[2026-09-26T05:05:17Z DEBUG sail_execution::task_runner::actor::handler] worker_task_status worker_id=1 job_id=42 stage=2 partition=0 attempt=0 status=RUNNING
[2026-09-26T05:05:17Z DEBUG sail_execution::task_runner::actor::handler] worker_task_status worker_id=1 job_id=42 stage=2 partition=0 attempt=0 status=SUCCEEDED
[2026-09-26T05:05:17Z DEBUG sail_execution::task_runner::actor::handler] worker_task_status worker_id=2 job_id=42 stage=2 partition=1 attempt=1 status=SUCCEEDED
[2026-09-26T05:05:17Z DEBUG sail_execution::task_runner::actor::handler] worker_task_status worker_id=3 job_id=42 stage=2 partition=2 attempt=0 status=FAILED
"""
        self.assertEqual(completed_worker_tasks(log), [
            dict(worker_id=1, job_id=42, stage=2, partition=0, attempt=0),
            dict(worker_id=2, job_id=42, stage=2, partition=1, attempt=1),
        ])


if __name__ == "__main__":
    unittest.main()
