import asyncio
import minerdaemon


class Monitor:
    """The monitor task is responsible for periodically reading statistics from
    the miner and generating a MonitorEvent that is dispatched through the
    event dispatcher."""

    def __init__(self, loop, miner, event_dispatcher, period, offset=0.0):
        """Create an instance of the Monitor.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            miner (minerhal.Miner): Miner.
            event_dispatcher (EventDispatcher): Event dispatcher.
            period (float): Period of monitoring in seconds.
            offset (float): Offset of monitoring start in seconds.

        Returns:
            Monitor: Instance of Monitor.

        """
        self._miner = miner
        self._event_dispatcher = event_dispatcher
        self._period = period
        self._offset = offset

        self._stop_event = asyncio.Event()
        self._task = loop.create_task(self.run())

    @asyncio.coroutine
    def run(self):
        """Run the monitor, which periodically reads statistics from the miner
        and generates a MonitorEvent for the event dispatcher."""

        stop_request = asyncio.Task(self._stop_event.wait())

        # Sleep for offset (or stop)
        done, pending = yield from asyncio.wait([asyncio.sleep(self._offset), stop_request], return_when=asyncio.FIRST_COMPLETED)

        while not self._stop_event.is_set():
            # Wait for sleep or stop
            done, pending = yield from asyncio.wait([asyncio.sleep(self._period), stop_request], return_when=asyncio.FIRST_COMPLETED)
            if stop_request in done:
                break

            # Read miner statistics
            stats = self._miner.read_stats()

            # Push to event dispatcher
            self._event_dispatcher.dispatch(minerdaemon.events.MonitorEvent(stats))

        # Cancel pending sleep
        for task in pending:
            task.cancel()

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        self._stop_event.set()
        yield from self._task
