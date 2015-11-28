import time
import minerhal
import asyncio
import enum

import minerdaemon.events


class ClientWork:
    """The interface to work provided by a client, for the WorkManager to use
    in deriving work, validating solutions, and submitting shares."""

    def derive_works(self, count):
        """Derive count number of unique works suitable for loading onto a
        minerhal.Miner.

        Args:
            count (int): Number of unique works to derive.

        Returns:
            list: List of minerhal.MinerWork instances

        """
        raise NotImplementedError()

    def validate_solution(self, solution):
        """Validate a solution to the work.

        Args:
            solution (minerhal.MinerSolution): Solution to validate.

        Returns:
            bool: True if the solution is valid, False if it is not.

        """
        raise NotImplementedError()

    @asyncio.coroutine
    def submit_share(self, solution, share_response_future):
        """Submit a solution with the client, and populate the provided asyncio
        future with the server classification of the submitted share (e.g.
        good, invalid, stale, etc.).

        Args:
            solution (minerhal.MinerSolution): Solution to submit.
            share_response_future (asyncio.Future): Future containing an
                instance of ClientShareResponse.

        """
        raise NotImplementedError()

    @property
    def difficulty(self):
        """float: Difficulty of this work."""
        raise NotImplementedError()


class ClientShareResponse:
    """A container for the classification of a submitted share, including the
    classification type and reason for classification. This is returned with
    the submit_share() routine in the ClientWork interface class."""

    class Classification(enum.Enum):
        """Share classification enumeration."""
        good = 0
        invalid = 1
        stale = 2
        duplicate = 3
        timeout = 4
        unauthorized = 5
        other = 6

    def __init__(self, classification, reason=None):
        """Create an instance of ClientShareResponse.

        Args:
            classification (ClientShareResponse.Classification): Share
                classification.
            reason (str): Reason string.

        Returns:
            ClientShareResponse: Instance of ClientShareResponse.

        """
        self.classification = classification
        self.reason = reason

    def __eq__(self, other):
        """Compare this ClientShareResponse to another for equality.

        Returns:
            bool: True if the same, False if different.

        """
        return self.classification == other.classification and self.reason == other.reason


class WorkManager:
    """The WorkManager task is responsible for keeping the current loaded work
    context and for wrapping the miner with the coroutines that load work onto
    the miner (triggered by the client) and read, validate, and submit
    solutions from the miner (triggered by the SolutionWatcher task)."""

    def __init__(self, loop, miner, event_dispatcher):
        """Create an instance of WorkManager.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            miner (minerhal.Miner): Miner.
            event_dispatcher (EventDispatcher): Event dispatcher.

        Returns:
            WorkManager: An instance of WorkManager.

        """
        self._miner = miner
        self._event_dispatcher = event_dispatcher

        self._previous_work = None
        self._current_work = None
        self._miner_start_timestamp = None

    @asyncio.coroutine
    def load_work(self, work):
        """Derive unique work from provided work and load it onto the miner.
        This coroutine timestamps the derivation and loading process and
        generates a WorkEvent for the event dispatcher.

        In case of hardware error, this solution generates a HardwareErrorEvent
        for the event dispatcher, wrapping the underlying error.

        Args:
            work (ClientWork): An instance of the ClientWork interface.

        """
        # Timestamp received
        received_timestamp = time.time()

        # Update work state
        self._previous_work = self._current_work
        self._current_work = work

        # Construct derived work
        derived_works = work.derive_works(self._miner.num_workers())

        # Timestamp derived
        derived_timestamp = time.time()

        # Save timestamp of approximate mining start
        self._miner_start_timestamp = derived_timestamp

        # Load work
        try:
            self._miner.load_work(derived_works)
        except minerhal.MinerError as e:
            self._event_dispatcher.dispatch(minerdaemon.events.HardwareErrorEvent("Loading work", e))
            return

        # Timestamp load
        loaded_timestamp = time.time()

        # Push load work event to event queue
        self._event_dispatcher.dispatch(minerdaemon.events.WorkEvent(received_timestamp, derived_timestamp, loaded_timestamp, work.difficulty, work))

    @asyncio.coroutine
    def read_solution(self):
        """Read a solution from the miner, validate it with the ClientWork interface,
        and submit it through the ClientWork interface. This coroutine also timestamps
        each step of solution process and generates a SolutionEvent for the
        event dispatcher.

        In case of hardware error, this solution generates a HardwareErrorEvent
        for the event dispatcher, wrapping the underlying error.

        """
        # Timestamp poll
        poll_timestamp = time.time()

        # Read the solution
        try:
            solution = self._miner.read_solution()
        except minerhal.MinerError as e:
            self._event_dispatcher.dispatch(minerdaemon.events.HardwareErrorEvent("Reading Solution", e))
            return

        # If found was a false alarm and no solution was read
        if solution is None:
            self._event_dispatcher.dispatch(minerdaemon.events.HardwareErrorEvent("Reading Solution", "Got found False alarm."))
            return

        # Adjust relative found_timestamp in solution with mining start timestamp
        found_timestamp = self._miner_start_timestamp + solution.found_timestamp

        # Timestamp read
        read_timestamp = time.time()

        # Validate the solution
        if self._current_work and self._current_work.validate_solution(solution):
            solved_work = self._current_work
            valid = True
        elif self._previous_work and self._previous_work.validate_solution(solution):
            solved_work = self._previous_work
            valid = True
        else:
            valid = False

        # Timestamp validated
        validated_timestamp = time.time()

        if valid:
            # Submit share
            share_response_future = asyncio.Future()
            yield from solved_work.submit_share(solution, share_response_future)
            submitted_timestamp = time.time()

            # Wait for share acknowledgement
            share_response = yield from share_response_future
            accepted = share_response.classification == ClientShareResponse.Classification.good

            # Timestamp acknowledged if share did not timeout
            if share_response.classification != ClientShareResponse.Classification.timeout:
                acknowledged_timestamp = time.time()
            else:
                acknowledged_timestamp = None
        else:
            submitted_timestamp = None
            share_response = None
            accepted = None
            acknowledged_timestamp = None

        # Push solution event to event dispatcher
        self._event_dispatcher.dispatch(minerdaemon.events.SolutionEvent(found_timestamp, poll_timestamp, read_timestamp, validated_timestamp, submitted_timestamp, acknowledged_timestamp, valid, accepted, share_response, solution))

    def supported_work(self):
        """Get the list of supported work class types by this miner, e.g.
        minerhal.MinerSimpleWork, minerhal.MinerBitshareWork, etc..

        Returns:
            list: List of supported work class types (subclass types of
                minerhal.MinerWork).

        """
        return self._miner.supported_work()

    def idle(self):
        """Idle the miner."""
        self._miner.idle()

    @asyncio.coroutine
    def stop(self):
        """Stop this task. Idles and resets the miner."""
        self._miner.idle()
        self._miner.reset()
