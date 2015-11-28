import asyncio

import minerdaemon.events
import minerdaemon.eventdispatcher


_ANSI_COLORS = {
    "black": "\x1b[1;30m",
    "red": "\x1b[1;31m",
    "green": "\x1b[1;32m",
    "yellow": "\x1b[1;33m",
    "blue": "\x1b[1;34m",
    "magenta": "\x1b[1;35m",
    "cyan": "\x1b[1;36m",
    "white": "\x1b[1;37m",
    "reset": "\x1b[0m",
}
"""ANSI escape color table."""


class LoggerBackend(minerdaemon.eventdispatcher.EventBackend):
    """Logger event backend to log human-readable representation of events to a
    logging.Logger."""

    def __init__(self, loop, logger, enable_colors=False):
        """Create an instance of LoggerBackend.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            logger (logging.Logger): Logger.
            enable_colors (bool): Enable ANSI colors in log.

        Returns:
            LoggerBackend: Instance of LoggerBackend.

        """
        self._logger = logger
        self._enable_colors = enable_colors

    def _format_event(self, event, desc, color):
        if color is not None and self._enable_colors:
            return _ANSI_COLORS[color] + "{:<20}".format("[" + event.__class__.__name__ + "]") + desc + _ANSI_COLORS["reset"]
        else:
            return "{:<20}".format("[" + event.__class__.__name__ + "]") + desc

    def _log_version_event(self, event):
        self._logger.info(self._format_event(event, "Started minerd v{}, miner {}.".format(event.version, event.miner), "yellow"))

    def _log_solution_event(self, event):
        desc = "Got solution. Valid {}".format(event.valid)
        if event.valid:
            desc += ", Accepted {}".format(event.accepted)
            if event.response is not None:
                desc += ", Classification {}".format(event.response.classification.name)
                if event.response.reason is not None:
                    desc += ", Reason {}".format(event.response.reason)

        color = "green" if event.valid and event.accepted else "red"

        self._logger.info(self._format_event(event, desc, color))
        self._logger.debug(self._format_event(event, str(event.solution), color))

    def _log_work_event(self, event):
        self._logger.info(self._format_event(event, "Loaded new work (pool difficulty {}).".format(event.difficulty), "blue"))
        self._logger.debug(self._format_event(event, str(event.work), "blue"))

    def _log_hardware_error_event(self, event):
        self._logger.error(self._format_event(event, "Context {}, Error {}".format(event.context, event.error), "red"))

    def _log_monitor_event(self, event):
        self._logger.info(self._format_event(event, str(event.statistics), "magenta"))

    def _log_statistics_event(self, event):
        self._logger.info(self._format_event(event, "Uptime {:.2f} seconds".format(event.statistics['uptime']), "magenta"))
        self._logger.info(self._format_event(event, "Shares: Accepted {}, Rejected {}, Invalid {}".format(event.statistics['accepted_shares'], event.statistics['rejected_shares'], event.statistics['invalid_shares']), "magenta"))
        self._logger.info(self._format_event(event, "Hashrate: 5m {:.2f}, 15m {:.2f}, 60m {:.2f} H/s".format(event.statistics['hashrate']['5min'], event.statistics['hashrate']['15min'], event.statistics['hashrate']['60min']), "magenta"))

    def _log_network_event(self, event):
        self._logger.info(self._format_event(event, "Status \"{}\", Connected {}, url {}, username \"{}\".".format(event.message, event.connected, event.url, event.username), "cyan"))

    def _log_shutdown_event(self, event):
        self._logger.info(self._format_event(event, "{}".format(event.message), "yellow"))

    def _log_generic_event(self, event):
        self._logger.info(self._format_event(event, str(event), "reset"))

    @asyncio.coroutine
    def event_occurred(self, event):
        if isinstance(event, minerdaemon.events.VersionEvent):
            self._log_version_event(event)
        elif isinstance(event, minerdaemon.events.SolutionEvent):
            self._log_solution_event(event)
        elif isinstance(event, minerdaemon.events.WorkEvent):
            self._log_work_event(event)
        elif isinstance(event, minerdaemon.events.HardwareErrorEvent):
            self._log_hardware_error_event(event)
        elif isinstance(event, minerdaemon.events.MonitorEvent):
            self._log_monitor_event(event)
        elif isinstance(event, minerdaemon.events.StatisticsEvent):
            self._log_statistics_event(event)
        elif isinstance(event, minerdaemon.events.NetworkEvent):
            self._log_network_event(event)
        elif isinstance(event, minerdaemon.events.ShutdownEvent):
            self._log_shutdown_event(event)
        else:
            self._log_generic_event(event)

    @asyncio.coroutine
    def stop_and_join(self):
        return
