import os
import os.path
import asyncio
import json

import minerdaemon.eventdispatcher


class TopClientHandler(asyncio.Protocol):
    """Client connection context and handler class for minertop."""

    def __init__(self, server):
        """Create an instance of TopClientHandler.

        Args:
            server (TopServerBackend): TopServerBackend associated with this
                client.

        Returns:
            TopClientHandler: Instance of TopClientHandler.

        """
        self._server = server
        self._transport = None

    def write_event(self, event):
        """Write a new event to the client.

        Args:
            event (Event): Instance of Event.

        """
        self._transport.write(json.dumps(event, cls=minerdaemon.events.EventEncoder).encode("utf-8") + b"\n")

    def close(self):
        """Close the client connection."""
        self._transport.close()

    def connection_made(self, transport):
        """asyncio.Protocol connection made callback.

        Args:
            transport (asyncio.BaseTransport): Transport.

        """
        self._transport = transport
        self._server.clients.append(self)

        # Write all recent events
        for event in self._server.recent_events.values():
            # If we're not about to send this in our cached events list
            if event not in self._server.cached_events:
                self.write_event(event)

        # Write cached events
        for cached_event in self._server.cached_events:
            self.write_event(cached_event)

    def connection_lost(self, exc):
        """asyncio.Protocol connection lost callback.

        Args:
            exc (Exception or None): Exception for connection loss.

        """
        self._server.clients.remove(self)

    def data_received(self, data):
        """asyncio.Protocol data received callback.

        Args:
            data (bytes): Data received.

        """
        pass


class TopServerBackend(minerdaemon.eventdispatcher.EventBackend):
    """minertop server event backend for real-time dispatching events to
    minertop clients."""

    _NUM_CACHED_EVENTS = 30
    """Number of cached events to deliver upon new minertop connections."""

    def __init__(self, loop, path):
        """Create an instance of TopServerBackend.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            path (str): UNIX socket path.

        Returns:
            TopServerBackend: Instance of TopServerBackend.

        """
        self._loop = loop
        self._path = path

        self.clients = []

        # Map of most recent events by type (event type -> event)
        self.recent_events = {}
        # List of most recent events
        self.cached_events = []

        # Clean up old socket
        if os.path.exists(path):
            os.remove(path)

        self._server = None
        loop.create_task(self._create_server())

    @asyncio.coroutine
    def _create_server(self):
        """Create the UNIX socket server, handling connecting clients with the
        TopClientHandler class."""

        self._server = yield from self._loop.create_unix_server(lambda: TopClientHandler(self), self._path)

        # Make socket permissive
        os.chmod(self._path, 0o777)

    @asyncio.coroutine
    def event_occurred(self, event):
        # Update the event type in recent events
        self.recent_events[type(event)] = event

        # Add the event to our cache
        self.cached_events.append(event)

        # Limit cached events list to NUM_CACHED_EVENTS
        if len(self.cached_events) > TopServerBackend._NUM_CACHED_EVENTS:
            self.cached_events = self.cached_events[1:]

        # Write the event to connected clients
        for client in self.clients:
            client.write_event(event)

    @asyncio.coroutine
    def stop_and_join(self):
        # Disconnect clients
        for client in self.clients:
            client.close()

        # Close server
        self._server.close()

        # Wait until server shuts down
        yield from self._server.wait_closed()

        # Delete socket
        os.remove(self._path)
