import json
import asyncio

import minerdaemon.events
import minerdaemon.eventdispatcher


class JsonLoggerBackend(minerdaemon.eventdispatcher.EventBackend):
    """JSON logger event backend to log line-delimited JSON-serialized events
    to a file."""

    def __init__(self, loop, jsonfile):
        """Create an instance of JsonLoggerBackend.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            jsonfile (File-like object): File to log to.

        Returns:
            JsonLoggerBackend: Instance of JsonLoggerBackend.

        """
        self._jsonfile = jsonfile

    @asyncio.coroutine
    def event_occurred(self, event):
        self._jsonfile.write(json.dumps(event, cls=minerdaemon.events.EventEncoder).encode('utf-8') + "\n")

    @asyncio.coroutine
    def stop_and_join(self):
        return
