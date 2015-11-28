import json
import asyncio
import aiohttp
import logging

import minerdaemon.events
import minerdaemon.eventdispatcher


logger = logging.getLogger('minerd')


class SplunkLoggerBackend(minerdaemon.eventdispatcher.EventBackend):
    """Splunk logger backend to log JSON-serialized events to a HTTP logging
    endpoint for Splunk analytics."""

    _NUM_EVENTS_BATCHED = 25
    """Number of events to batch per HTTP post."""

    def __init__(self, loop, url):
        """Create an instance of SplunkLoggerBackend.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            url (str): HTTP logging endpoint URL.

        Returns:
            SplunkLoggerBackend: Instance of SplunkLoggerBackend.

        """
        self._url = url
        self._events = []
        self._version = None
        self._miner = None
        self._username = None

    @asyncio.coroutine
    def event_occurred(self, event):
        if isinstance(event, minerdaemon.events.VersionEvent):
            # Save version and miner if this is a VersionEvent
            self._version = event.version
            self._miner = event.miner
        elif isinstance(event, minerdaemon.events.NetworkEvent):
            # Save username if this is a NetworkEvent
            self._username = event.username

        self._events.append(event)

        # Log events to endpoint, if we've reached the batch event count
        if self._version and self._miner and self._username and len(self._events) >= self._NUM_EVENTS_BATCHED:
            # Assemble payload
            payload = [{'channel': 'minerd', 'version': self._version, 'miner': self._miner, 'username': self._username, 'event': e} for e in self._events]
            payload = json.dumps(payload, cls=minerdaemon.events.EventEncoder)

            # Reset events list
            self._events = []

            # POST to endpoint
            try:
                r = yield from aiohttp.post(self._url, params={'bulk': 'true'}, data=payload)
                r.close()
            except Exception as e:
                logger.debug("[splunkloggerbackend] Error logging to endpoint: %s", str(e))

    @asyncio.coroutine
    def stop_and_join(self):
        return
