import time
import asyncio
import statistics

import minerdaemon.eventdispatcher


class CounterStatistic:
    """Counter statistic that represents a scalar counter."""

    def __init__(self, initial_value=0):
        """Create an instance of CounterStatistic.

        Args:
            initial_value (int): Initial value.

        Returns:
            CounterStatistic: Instance of CounterStatistic.

        """
        self._value = initial_value

    def update(self, delta=1):
        """Increment the counter by a delta.

        Args:
            delta (int): Delta to increment counter with.

        """
        self._value += delta

    @property
    def value(self):
        """int: Current counter value."""
        return self._value


class RunningStatistic:
    """Running statistic that contains the running min, max, mean, median,
    and variance statistic of a scalar over a window."""

    def __init__(self, window_length):
        """Create an instance of RunningStatistic.

        Args:
            window_length (int): Window length in number of samples to compute
                statistics over.

        Returns:
            RunningStatistic: Instance of RunningStatistic.

        """
        self._values = []
        self._window_length = window_length

    def update(self, value):
        """Update the running statistc with the latest value.

        Args:
            value (float): Latest value.

        """
        self._values.append(value)
        if len(self._values) > self._window_length:
            self._values = self._values[1:]

    @property
    def value(self):
        """dict: Dictionary with keys 'min', 'max', 'mean', 'median',
        and 'variance' containing the running statistics of this value."""

        if len(self._values) == 0:
            return {'min': None, 'max': None, 'mean': None, 'median': None, 'variance': None}

        smin = min(self._values)
        smax = max(self._values)
        smean = statistics.mean(self._values)
        smedian = statistics.median(self._values)
        svariance = statistics.variance(self._values) if len(self._values) > 1 else None

        return {'min': smin, 'max': smax, 'mean': smean, 'median': smedian, 'variance': svariance}


class HashrateStatistic:
    """Hashrate statistic that calculates the running hashrate over a
    window."""

    def __init__(self, window_duration):
        """Create an instance of HashrateStatistic.

        Args:
            window_duration (float): Window duration to calculate hashrate over
                in seconds.

        Returns:
            HashrateStatistic: Instance of HashrateStatistic.

        """
        self._difficulty = None
        self._window_duration = window_duration
        self._solution_timestamps = []

    def update(self, event):
        """Update hashrate with a solution or work event.

        Args:
            event (SolutionEvent or WorkEvent): Event to update hashrate.

        """
        if isinstance(event, minerdaemon.events.SolutionEvent):
            if event.valid and event.accepted:
                self._solution_timestamps.append(event.found_timestamp)
        elif isinstance(event, minerdaemon.events.WorkEvent):
            self._difficulty = event.difficulty

    def prune(self, timestamp):
        """Prune solutions outside of calculation window.

        Args:
            timestamp (float): Current timestamp.

        """
        cutoff_timestamp = timestamp - self._window_duration
        self._solution_timestamps = list(filter(lambda t: t > cutoff_timestamp, self._solution_timestamps))

    @property
    def value(self):
        """float: Hashrate in H/s."""

        if self._difficulty is None:
            return 0.0

        # This calculation assumes that the difficulty has been constant for
        # the entire time window.
        return (len(self._solution_timestamps) * self._difficulty * 2 ** 32) / self._window_duration


def _diff(values):
    """Compute the first difference of values.

    Args:
        values (list): List of int or float values.

    Returns:
        list: first-difference of values.

    """
    d = []
    for i in range(1, len(values)):
        if values[i] is None:
            d.append(None)
        else:
            d.append(values[i] - values[i - 1])
    return d


class StatisticsBackend(minerdaemon.eventdispatcher.EventBackend):
    """Statistics event backend to compute mining statistics from events and
    periodically aggregate them into a StatisticsEvent that is dispatched
    through the event dispatcher."""

    def __init__(self, loop, event_dispatcher, period, offset=0.0):
        """Create an instance of StatisticsBackend.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            event_dispatcher (EventDispatcher): Event dispatcher.
            period (float): Period of monitoring in seconds.
            offset (float): Offset of monitoring start in seconds.

        Returns:
            StatisticsBackend: Instance of StatisticsBackend.

        """
        self._loop = loop
        self._event_dispatcher = event_dispatcher
        self._period = period
        self._offset = offset

        self._start_time = time.time()

        self._solution_poll_latency_statistic = RunningStatistic(100)
        self._solution_read_latency_statistic = RunningStatistic(100)
        self._solution_validate_latency_statistic = RunningStatistic(100)
        self._solution_submit_latency_statistic = RunningStatistic(100)
        self._solution_acknowledge_latency_statistic =  RunningStatistic(100)
        self._work_derive_latency_statistic = RunningStatistic(100)
        self._work_load_latency_statistic = RunningStatistic(100)
        self._accepted_shares_counter = CounterStatistic()
        self._rejected_shares_counter = CounterStatistic()
        self._invalid_shares_counter = CounterStatistic()
        self._hashrate_5min = HashrateStatistic(5*60)
        self._hashrate_15min = HashrateStatistic(15*60)
        self._hashrate_60min = HashrateStatistic(60*60)

        self._stop_event = asyncio.Event()
        self._task = loop.create_task(self._run())

    @asyncio.coroutine
    def event_occurred(self, event):
        if isinstance(event, minerdaemon.events.SolutionEvent):
            timestamps = [event.found_timestamp, event.poll_timestamp, event.read_timestamp, event.validated_timestamp, event.submitted_timestamp, event.acknowledged_timestamp]
            (poll_latency, read_latency, validate_latency, submit_latency, acknowledge_latency) = _diff(timestamps)

            self._solution_poll_latency_statistic.update(poll_latency)
            self._solution_read_latency_statistic.update(read_latency)
            self._solution_validate_latency_statistic.update(validate_latency)

            if submit_latency is not None:
                self._solution_submit_latency_statistic.update(submit_latency)

            if acknowledge_latency is not None:
                self._solution_acknowledge_latency_statistic.update(acknowledge_latency)

            if event.accepted is not None:
                self._accepted_shares_counter.update(event.accepted)
                self._rejected_shares_counter.update(not event.accepted)

            self._invalid_shares_counter.update(not event.valid)

            self._hashrate_5min.update(event)
            self._hashrate_15min.update(event)
            self._hashrate_60min.update(event)

        elif isinstance(event, minerdaemon.events.WorkEvent):
            timestamps = [event.received_timestamp, event.derived_timestamp, event.loaded_timestamp]
            (derive_latency, load_latency) = _diff(timestamps)

            self._work_derive_latency_statistic.update(derive_latency)
            self._work_load_latency_statistic.update(load_latency)

            self._hashrate_5min.update(event)
            self._hashrate_15min.update(event)
            self._hashrate_60min.update(event)

    @asyncio.coroutine
    def _run(self):
        """Run a loop to periodically dispatch a StatisticsEvent to the event
        dispatcher, containing the latest aggregated statistics for that
        period."""

        stop_request = asyncio.Task(self._stop_event.wait())

        # Sleep for offset (or stop)
        done, pending = yield from asyncio.wait([asyncio.sleep(self._offset), stop_request], return_when=asyncio.FIRST_COMPLETED)

        while not self._stop_event.is_set():
            # Wait for sleep or stop
            done, pending = yield from asyncio.wait([asyncio.sleep(self._period), stop_request], return_when=asyncio.FIRST_COMPLETED)
            if stop_request in done:
                break

            timestamp = time.time()

            # Prune expired shares in our windowed hashrate statistics
            self._hashrate_5min.prune(timestamp)
            self._hashrate_15min.prune(timestamp)
            self._hashrate_60min.prune(timestamp)

            stats = {
                'uptime': timestamp - self._start_time,
                'latency': {
                    'solution': {
                        'poll': self._solution_poll_latency_statistic.value,
                        'read': self._solution_read_latency_statistic.value,
                        'validate': self._solution_validate_latency_statistic.value,
                        'submit': self._solution_submit_latency_statistic.value,
                        'acknowledge': self._solution_acknowledge_latency_statistic.value,
                    },
                    'work': {
                        'derive': self._work_derive_latency_statistic.value,
                        'load': self._work_load_latency_statistic.value,
                    },
                },
                'accepted_shares': self._accepted_shares_counter.value,
                'rejected_shares': self._rejected_shares_counter.value,
                'invalid_shares': self._invalid_shares_counter.value,
                'hashrate': {
                    '5min': self._hashrate_5min.value,
                    '15min': self._hashrate_15min.value,
                    '60min': self._hashrate_60min.value,
                },
            }
            self._event_dispatcher.dispatch(minerdaemon.events.StatisticsEvent(stats))

        # Cancel pending sleep
        for task in pending:
            task.cancel()

    @asyncio.coroutine
    def stop_and_join(self):
        self._stop_event.set()
        yield from self._task
