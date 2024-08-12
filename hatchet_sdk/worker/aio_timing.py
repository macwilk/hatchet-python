import asyncio
import contextlib
import selectors
import time
from selectors import SelectorKey
from typing import Any


class TimedSelector(selectors.DefaultSelector):
    """
    A selector is a python module built for high-level and efficient I/O multiplexing. Since
    there are multiple types of selectors, we subclass the default one which is determined
    based on our platform.

    This function extends the default selector so we track the timing of our IO per function since
    traditional timeit calls would not measure this.
    """

    select_time = 0.0

    def reset_select_time(self) -> None:
        """Reset the selector timing."""
        self.select_time = 0.0

    def select(self, timeout: int = None) -> list[tuple[SelectorKey, Any]]:
        """
        Hook the selector's 'select' function to enable timing for it.

        :param timeout: timeout to pass to the selector for running coroutines.
        :returns: result of parent function
        """
        if not timeout or timeout <= 0:
            return super().select(timeout)
        start = time.time()
        try:
            return super().select(timeout)
        finally:
            self.select_time += time.time() - start


@contextlib.contextmanager
def print_timing(desc: str = None) -> None:
    """
    Enables fine-grain timing of AsyncIO calls to measure various timings of blocking and
        non-blocking calls.

    :param desc: the description of the block of code being timed, acts as a header for the
        logged block.
    """
    selector = asyncio.get_running_loop()._selector
    selector.reset_select_time()

    real_time = time.time()
    process_time = time.process_time()

    yield
    real_time = time.time() - real_time
    cpu_time = time.process_time() - process_time
    select_time = asyncio.get_running_loop()._selector.select_time
    other_io_time = max(0.0, real_time - cpu_time - select_time)

    if desc:
        print(f"---{desc}---")
    print(f"CPU time:      {cpu_time:.3f} s")
    print(f"Select time:   {select_time:.3f} s")
    print(f"Other IO time: {other_io_time:.3f} s")
    print(f"Real time:     {real_time:.3f} s")
    print("")
