import json
import time
import collections

from minerdaemon.workmanager import ClientShareResponse


class EventEncoder(json.JSONEncoder):
    """A json.JSONEncoder interface implementation for serializing Event
    objects to JSON."""

    def default(self, obj):
        """Serialize obj to JSON."""

        if isinstance(obj, Event):
            return obj.to_json_obj()
        return json.JSONEncoder.default(self, obj)


class Event:
    """Event base class."""

    def __init__(self):
        """Create an Event instance.

        Returns:
            Event: Instance of Event.

        """
        self._timestamp = time.time()

    def _to_json_obj(self):
        """Serialize this event payload to a JSON-serializable object.

        Returns:
            Object: JSON-serializable object.

        """
        raise NotImplementedError()

    def to_json_obj(self):
        """Serialize this event to a JSON-serializable object.

        Returns:
            dict: JSON-serializable dictionary.

        """
        return {"type": self.__class__.__name__, "timestamp": self._timestamp, "payload": self._to_json_obj()}

    def to_json(self):
        """Serialize this event to JSON.

        Returns:
            str: JSON-serializd string.

        """
        return json.dumps(self.to_json_obj())

    @staticmethod
    def from_json(s):
        """Deserialize an event from JSON.

        Args:
            s (str): JSON-serializd string.

        Returns:
            Event: Instance of Event.

        """
        obj = json.loads(s)

        # Validate object
        if not isinstance(obj, dict) or "type" not in obj or \
                "timestamp" not in obj or "payload" not in obj:
            raise ValueError("Invalid event.")

        # Look up event class
        try:
            cls = globals()[obj["type"]]
        except KeyError:
            raise ValueError("Unknown event '{}'".format(obj["type"]))

        # Create derived event
        event = cls._from_json_obj(obj["payload"])

        # Update timestamp with timestamp in JSON object
        event._timestamp = obj["timestamp"]

        return event

    @staticmethod
    def _from_json_obj(payload):
        """Deserialize an event from JSON-serializable object.

        Args:
            payload (object): JSON-serializable object of the payload.

        Returns:
            Event: A derived instance of Event.

        """
        raise NotImplementedError()

    @property
    def timestamp(self):
        """float: UNIX timestamp of event creation."""
        return self._timestamp

    @property
    def type(self):
        """str: Event type."""
        return self.__class__.__name__


class VersionEvent(Event):
    """Version event containing software version and miner driver name."""

    def __init__(self, version, miner):
        """Create an instance of VersionEvent.

        Args:
            version (str): Software version string.
            miner (str): Miner driver name.

        Returns:
            VersionEvent: Instance of VersionEvent.

        """
        Event.__init__(self)
        self.version = version
        self.miner = miner

    def _to_json_obj(self):
        return {
            "version": self.version,
            "miner": self.miner
        }

    @staticmethod
    def _from_json_obj(payload):
        return VersionEvent(
            version=payload["version"],
            miner=payload["miner"]
        )


class SolutionEvent(Event):
    """Solution event for the solution found by a miner, containing the
    timestamps of the solution process (found, polled, read, validated,
    submitted, acknowledged), the validity of solution, the accepted status of
    solution, the pool classification of solution, and the miner solution
    itself."""

    def __init__(self, found_timestamp, poll_timestamp, read_timestamp, validated_timestamp, submitted_timestamp, acknowledged_timestamp, valid, accepted, response, solution):
        """Create an instance of SolutionEvent.

        Args:
            found_timestamp (float): UNIX timestamp of solution was found in
                the miner.
            poll_timestamp (float): UNIX timestamp of solution was detected.
            read_timestamp (float): UNIX timestamp of solution was read from
                the miner.
            validated_timestamp (float): UNIX timestamp of solution was
                validated.
            submitted_timestamp (float): UNIX timestamp of solution was
                submitted to pool.
            acknowledged_timestamp (float): UNIX timestamp of solution
                acknowledged by pool.
            valid (bool): Solution is valid.
            accepted (bool): Solution was accepted by pool.
            response (ClientShareResponse): Solution classification by pool.
            solution (minerhal.MinerSolution): Solution object.

        Returns:
            SolutionEvent: Instance of SolutionEvent.

        """
        Event.__init__(self)
        self.found_timestamp = found_timestamp
        self.poll_timestamp = poll_timestamp
        self.read_timestamp = read_timestamp
        self.validated_timestamp = validated_timestamp
        self.submitted_timestamp = submitted_timestamp
        self.acknowledged_timestamp = acknowledged_timestamp
        self.valid = valid
        self.accepted = accepted
        self.response = response
        self.solution = solution

    def _to_json_obj(self):
        return {
            "timestamps": {
                "found": self.found_timestamp,
                "poll": self.poll_timestamp,
                "read": self.read_timestamp,
                "validated": self.validated_timestamp,
                "submitted": self.submitted_timestamp,
                "acknowledged": self.acknowledged_timestamp
            },
            "valid": self.valid,
            "accepted": self.accepted,
            "response": {
                "classification": self.response.classification.name,
                "reason": self.response.reason
            } if self.response is not None else None
        }

    @staticmethod
    def _from_json_obj(payload):
        return SolutionEvent(
            found_timestamp=payload["timestamps"]["found"],
            poll_timestamp=payload["timestamps"]["poll"],
            read_timestamp=payload["timestamps"]["read"],
            validated_timestamp=payload["timestamps"]["validated"],
            submitted_timestamp=payload["timestamps"]["submitted"],
            acknowledged_timestamp=payload["timestamps"]["acknowledged"],
            valid=payload["valid"],
            accepted=payload["accepted"],
            response=ClientShareResponse(
                classification=getattr(ClientShareResponse.Classification, payload["response"]["classification"]),
                reason=payload["response"]["reason"]
            ) if payload["response"] is not None else None,
            solution=None
        )


class WorkEvent(Event):
    """Work event for the work loaded to the miner, containing the timestamps
    of the work loading process (received, derived, loaded), the difficulty of
    the work, and the work object itself."""

    def __init__(self, received_timestamp, derived_timestamp, loaded_timestamp, difficulty, work):
        """Create an instance of WorkEvent.

        Args:
            received_timestamp (float): UNIX timestamp of work received from
                client.
            derived_timestamp (float): UNIX timestamp of unique works derived.
            loaded_timestamp (float): UNIX timestamp of unique works loaded to
                miner.
            difficulty (float): Difficulty of the work.
            work (ClientWork): Work loaded.

        Returns:
            WorkEvent: Instance of WorkEvent.

        """
        Event.__init__(self)
        self.received_timestamp = received_timestamp
        self.derived_timestamp = derived_timestamp
        self.loaded_timestamp = loaded_timestamp
        self.difficulty = difficulty
        self.work = work

    def _to_json_obj(self):
        return {
            "timestamps": {
                "received": self.received_timestamp,
                "derived": self.derived_timestamp,
                "loaded": self.loaded_timestamp
            },
            "difficulty": self.difficulty
        }

    @staticmethod
    def _from_json_obj(payload):
        return WorkEvent(
            received_timestamp=payload["timestamps"]["received"],
            derived_timestamp=payload["timestamps"]["derived"],
            loaded_timestamp=payload["timestamps"]["loaded"],
            difficulty=payload["difficulty"],
            work=None
        )


class HardwareErrorEvent(Event):
    """Hardware error event, contains the context and error that occurred."""

    def __init__(self, context, error):
        """Create an instance of  HardwareErrorEvent.

        Args:
            context (str): Context of hardware error.
            error (str): Hardware error string.

        Returns:
            HardwareErrorEvent: Instance of HardwareErrorEvent.

        """
        Event.__init__(self)
        self.context = context
        self.error = str(error)

    def _to_json_obj(self):
        return {
            "context": self.context,
            "error": self.error
        }

    @staticmethod
    def _from_json_obj(payload):
        return HardwareErrorEvent(
            context=payload["context"],
            error=payload["error"]
        )


class MonitorEvent(Event):
    """Monitor event, containing statistics from the miner."""

    def __init__(self, statistics):
        """Create an instance of MonitorEvent.

        Args:
            statistics (collections.OrderedDict): Miner-specific statistics.

        Returns:
            MonitorEvent: Instance of MonitorEvent.

        """
        Event.__init__(self)
        self.statistics = statistics

    def _to_json_obj(self):
        # Transform ordered dictionary into array of tuples, and translate
        # Exception into an JSON-serializable string.
        statistics_array = []
        for key, value in self.statistics.items():
            if isinstance(value, Exception):
                statistics_array.append((key, "error: " + str(value)))
            else:
                statistics_array.append((key, value))

        return {
            "statistics": statistics_array
        }

    @staticmethod
    def _from_json_obj(payload):
        return MonitorEvent(
            statistics=collections.OrderedDict(payload["statistics"])
        )


class StatisticsEvent(Event):
    """Statistics event, containing statistics pertaining to uptime, shares,
    work loaded and hashrate."""

    def __init__(self, statistics):
        """Create an instance of StatisticsEvent.

        Args:
            statistics (dict): Dictionary of statistics containing,

            {
                'uptime': <uptime in seconds (float)>
                'latency': {
                    'solution': {
                        'poll': <poll latency in seconds (float)>,
                        'read': <read latency in seconds (float)>,
                        'validate': <validate latency in seconds (float)>,
                        'submit': <submit latency in seconds (float)>,
                        'acknowledge': <acknowledge latency in seconds (float)>,
                    },
                    'work': {
                        'derive': <derive latency in seconds (float)>,
                        'load': <load latency in seconds (float)>,
                    },
                },
                'accepted_shares': <number of accepted shares (int)>,
                'rejected_shares': <number of rejected shares (int)>,
                'invalid_shares': <number of invalid shares (int)>,
                'hashrate': {
                    '5min': <adjusted hashrate over last 5 min in H/s (float)>,
                    '15min': <adjusted hashrate over last 15 min in H/s (float)>,
                    '60min': <adjusted hashrate over last 60 min in H/s (float)>,
                },
            }

        Returns:
            StatisticsEvent: Instance of StatisticsEvent.

        """
        Event.__init__(self)
        self.statistics = statistics

    def _to_json_obj(self):
        return {
            "statistics": self.statistics
        }

    @staticmethod
    def _from_json_obj(payload):
        return StatisticsEvent(
            statistics=payload["statistics"]
        )


class NetworkEvent(Event):
    """Network event for network connectivity changes, and containing the
    connected status, pool URL, username, and connectivity change message."""

    def __init__(self, connected, url, username, message):
        """Create an instance of NetworkEvent.

        Args:
            connected (bool): Connected status.
            url (str): Pool URL.
            username (str): Pool username.
            message (str): Connectivity change message.

        Returns:
            NetworkEvent: Instance of NetworkEvent.

        """
        Event.__init__(self)
        self.connected = connected
        self.url = url
        self.username = username
        self.message = message

    def _to_json_obj(self):
        return {
            "connected": self.connected,
            "url": self.url,
            "username": self.username,
            "message": self.message
        }

    @staticmethod
    def _from_json_obj(payload):
        return NetworkEvent(
            connected=payload["connected"],
            url=payload["url"],
            username=payload["username"],
            message=payload["message"]
        )


class ShutdownEvent(Event):
    """Shutdown event for components of the daemon shutting down."""

    def __init__(self, message):
        """Create an instance of ShutdownEvent.

        Args:
            message (str): Message.

        Returns:
            ShutdownEvent: Instance of ShutdownEvent.

        """
        Event.__init__(self)
        self.message = message

    def _to_json_obj(self):
        return {
            "message": self.message
        }

    @staticmethod
    def _from_json_obj(payload):
        return ShutdownEvent(
            message=payload["message"]
        )
