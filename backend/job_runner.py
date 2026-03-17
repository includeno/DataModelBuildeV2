import threading
from typing import Any, Callable, Dict, Optional


class JobExecutionError(Exception):
    def __init__(self, code: str, message: str, *, category: str = "system_error"):
        super().__init__(message)
        self.code = code
        self.message = message
        self.category = category


class JobCanceledError(JobExecutionError):
    def __init__(self, message: str = "Job canceled"):
        super().__init__("JOB_CANCELED", message, category="user_error")


class ProjectJobRunner:
    def __init__(
        self,
        collab_storage: Any,
        process_job: Callable[[Dict[str, Any], Callable[[str], bool]], Dict[str, Any]],
        *,
        poll_interval_s: float = 0.1,
        max_running_per_project: int = 1,
    ):
        self.collab_storage = collab_storage
        self.process_job = process_job
        self.poll_interval_s = poll_interval_s
        self.max_running_per_project = max_running_per_project
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._stop_event = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="project-job-runner", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
            self._thread = None
        if thread and thread.is_alive():
            thread.join(timeout=1.0)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            job = self.collab_storage.claim_next_project_job(
                max_running_per_project=self.max_running_per_project
            )
            if not job:
                self._stop_event.wait(self.poll_interval_s)
                continue
            self._process_claimed_job(job)

    def _process_claimed_job(self, job: Dict[str, Any]) -> None:
        job_id = job["id"]
        try:
            self.collab_storage.update_project_job_progress(job_id, 10, status="running")
            result = self.process_job(job, self.is_cancel_requested)
            if self.is_cancel_requested(job_id):
                self.collab_storage.mark_project_job_canceled(job_id)
                return
            self.collab_storage.complete_project_job(job_id, result or {})
        except JobCanceledError:
            self.collab_storage.mark_project_job_canceled(job_id)
        except JobExecutionError as exc:
            self.collab_storage.fail_project_job(
                job_id,
                error_code=exc.code,
                error_category=exc.category,
                error_message=exc.message,
            )
        except Exception as exc:
            self.collab_storage.fail_project_job(
                job_id,
                error_code="JOB_INTERNAL_ERROR",
                error_category="system_error",
                error_message=str(exc),
            )

    def is_cancel_requested(self, job_id: str) -> bool:
        job = self.collab_storage.get_project_job_internal(job_id)
        return bool(job and job.get("cancelRequested"))
