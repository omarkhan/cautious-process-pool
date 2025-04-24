import resource
from concurrent.futures import ProcessPoolExecutor


class CautiousProcessPoolExecutor(ProcessPoolExecutor):
    def __init__(self, *args, memory_usage_target_mb, **kwargs):
        super().__init__(*args, **kwargs)

        if not self._safe_to_dynamically_spawn_children:
            raise ValueError(
                "CautiousProcessPoolExecutor must be used with a"
                " multiprocessing start method that allows dynamically"
                " spawning children"
            )

        self._memory_usage_target_mb = memory_usage_target_mb

    def _spawn_process(self):
        if self._memory_usage_within_limits():
            super()._spawn_process()

    def _memory_usage_within_limits(self):
        # TODO: check that this is in mb on all platforms
        child_process_memory_usage_mb = (
            resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024**2
        )
        return child_process_memory_usage_mb < self._memory_usage_target_mb
