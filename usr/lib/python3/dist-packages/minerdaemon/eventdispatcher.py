import asyncio


class EventBackend:
    """The event backend interface that processes an event disptached to it by
    the event dispatcher."""

    @asyncio.coroutine
    def event_occurred(self, event):
        """Handler for an event.

        Args:
            event (Event): Event that occurred.

        """
        raise NotImplementedError()

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        raise NotImplementedError()


class EventDispatcher:
    """The EventDispatcher task is responsible for dispatching an event to all
    event backends registered with it. Event backends implement the
    EventBackend interface."""

    def __init__(self, loop):
        """Create an instance of EventDispatcher.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.

        Returns:
            EventDispatcher: An instance of EventDispatcher.

        """
        self._event_queue = asyncio.Queue()
        self._backends = []
        self._stop_event = asyncio.Event()
        self._task = loop.create_task(self.run())

    def add_backend(self, backend):
        """Register a new event backend with this event dispatcher.

        Args:
            backend (EventBackend): Event backend to add.

        """
        self._backends.append(backend)

    def dispatch(self, event):
        """Dispatch an event to all event backends.

        Args:
            event (Event): Event to dispatch.

        """
        self._event_queue.put_nowait(event)

    @asyncio.coroutine
    def run(self):
        """Run the event dispatcher, which asynchronously consumes an event
        from the event queue and dispatches it to all event backends."""
        stop_request = asyncio.Task(self._stop_event.wait())

        while True:
            # Wait for get from event queue or stop
            queue_get = asyncio.Task(self._event_queue.get())
            done, pending = yield from asyncio.wait([queue_get, stop_request], return_when=asyncio.FIRST_COMPLETED)

            # Dispatch event to all backends
            if queue_get in done:
                event = queue_get.result()
                for backend in self._backends:
                    yield from backend.event_occurred(event)

            # Break if stop requested
            if stop_request in done:
                break

        # Cancel pending queue_get
        for task in pending:
            task.cancel()

        # Flush out queue to all backends
        while not self._event_queue.empty():
            event = self._event_queue.get_nowait()
            for backend in self._backends:
                yield from backend.event_occurred(event)

        # Stop and join event backends
        for backend in self._backends:
            yield from backend.stop_and_join()

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        self._stop_event.set()
        yield from self._task
