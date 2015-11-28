import asyncio
import threading

import minerhal


class SolutionWatcher:
    """The SolutionWatcher task is responsible for monitoring the miner for
    solutions and triggering the read_solution() coroutine in the work manager
    when a solution is detected."""

    def __init__(self, loop, miner, work_manager, event_dispatcher):
        """Create an instance of SolutionWatcher.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            miner (minerhal.Miner): Miner.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.

        Returns:
            SolutionWatcher: An instance of SolutionWatcher.

        """
        self._loop = loop
        self._miner = miner
        self._work_manager = work_manager

        self._read_done_event = threading.Event()
        self._stop_event = threading.Event()
        self._stopped_event = asyncio.Event()

        self._poll_thread = threading.Thread(target=self._run)
        self._loop.call_soon(self._poll_thread.start)

    @asyncio.coroutine
    def _call_read_solution(self):
        """Call read_solution() on the work manager, from the asyncio event
        loop's context."""
        yield from self._work_manager.read_solution()
        self._read_done_event.set()

    def _run(self):
        """Monitor the miner for solutions and schedule _call_read_solution()
        in the asyncio event loop's context when one occurs."""
        while not self._stop_event.is_set():
            # Poll for found with 500ms timeout
            if self._miner.poll_found(500):
                self._loop.call_soon_threadsafe(asyncio.async, self._call_read_solution())
                self._read_done_event.wait()
                self._read_done_event.clear()

        # Acknowledge stop
        self._loop.call_soon_threadsafe(self._stopped_event.set)

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        self._stop_event.set()
        yield from self._stopped_event.wait()
