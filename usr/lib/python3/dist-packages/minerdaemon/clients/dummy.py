import os
import time
import asyncio
import urllib.parse
import hashlib

import minerhal
import sha256
from . import utils

import minerdaemon.events
from minerdaemon.workmanager import ClientWork, ClientShareResponse


class DummySimpleWork(ClientWork):
    """The DummyClient implementation of the ClientWork interface for deriving
    MinerSimpleWork."""

    def __init__(self, client, difficulty):
        """Create an instance of DummySimpleWork.

        Args:
            client (DummyClient): Client associated with this work.
            difficulty (float): Difficulty of work to generate.

        Returns:
            DummySimpleWork: Instance of DummySimpleWork.

        """
        self._client = client
        self._difficulty = difficulty
        self._bits_comp = utils.difficulty_to_bits(difficulty)
        self._target_comp = utils.bits_to_target(self._bits_comp).to_bytes(32, byteorder='little')
        self._derived_works = {}

    def derive_works(self, count):
        works = []

        for _ in range(count):
            # Generate random work
            work = minerhal.MinerSimpleWork(
                midstate=os.urandom(32),
                merkle_lsw=os.urandom(4),
                bits=self._bits_comp,
                timestamp=int(time.time()),
                bits_comp=self._bits_comp
            )

            # Add it to our work list
            works.append(work)

            # Save this work under merkle_lsw for later validation
            self._derived_works[work.merkle_lsw] = work

        return works

    def validate_solution(self, solution):
        # Validate solution parameters match loaded work
        if solution.merkle_lsw not in self._derived_works \
                or self._derived_works[solution.merkle_lsw].midstate != solution.midstate \
                or self._derived_works[solution.merkle_lsw].bits != solution.bits \
                or (self._derived_works[solution.merkle_lsw].timestamp & 0xfffffff0) != (solution.timestamp & 0xfffffff0):
            return False

        # Form rest of block header
        block_header_rest = solution.merkle_lsw + solution.timestamp.to_bytes(4, 'little') + solution.bits.to_bytes(4, 'little') + solution.nonce.to_bytes(4, 'little')

        # First SHA256 hash, starting from midstate
        h = sha256.sha256()
        h.state = (solution.midstate, 64)
        h.update(block_header_rest)
        block_header_hash1 = h.digest()

        # Second SHA256 hash
        block_header_hash2 = hashlib.sha256(block_header_hash1).digest()

        # Compare block header hash to target
        return block_header_hash2[::-1] < self._target_comp[::-1]

    @asyncio.coroutine
    def submit_share(self, solution, share_response_future):
        yield from self._client.submit_share(solution, share_response_future)

    @property
    def difficulty(self):
        return self._difficulty


class DummyBitshareWork(ClientWork):
    """The DummyClient implementation of the ClientWork interface for deriving
    MinerBitshareWork."""

    def __init__(self, client, difficulty):
        """Create an instance of DummySimpleWork.

        Args:
            client (DummyClient): Client associated with this work.
            difficulty (float): Difficulty of work to generate.

        Returns:
            DummySimpleWork: Instance of DummySimpleWork.

        """
        self._client = client
        self._difficulty = difficulty
        self._bits_comp = utils.difficulty_to_bits(difficulty)
        self._target_comp = utils.bits_to_target(self._bits_comp).to_bytes(32, byteorder='little')
        self._derived_work = {}

        # Generate random block header
        self._block_header = minerhal.MinerBitshareWork.BlockHeader(
            version=3,
            prev_block_hash=os.urandom(32),
            bits=self._bits_comp,
            timestamp=int(time.time()),
            bits_comp=self._bits_comp
        )

        # Generate random merkle edge
        self._merkle_edge = minerhal.MinerBitshareWork.MerkleEdge(
            [os.urandom(32) for i in range(5)]
        )

    def derive_works(self, count):
        works = []

        for _ in range(count):
            # Generate random coinbase
            coinbase = minerhal.MinerBitshareWork.Coinbase(
                midstate=os.urandom(32),
                wallet_id=0,
                block_height=368774,
                lock_time=0,
                data_length=320
            )

            works.append(minerhal.MinerBitshareWork(self._block_header, self._merkle_edge, coinbase))

        return works

    def validate_solution(self, solution):
        # Validate solution parameters match loaded work for this worker
        if self._block_header.bits != solution.bits or (self._block_header.timestamp & 0xffffff00) != (solution.timestamp & 0xffffff00):
            return False

        # Form rest of block header
        block_header_rest = solution.merkle_lsw + solution.timestamp.to_bytes(4, 'little') + solution.bits.to_bytes(4, 'little') + solution.nonce.to_bytes(4, 'little')

        # First SHA256 hash, starting from midstate
        h = sha256.sha256()
        h.state = (solution.midstate, 64)
        h.update(block_header_rest)
        block_header_hash1 = h.digest()

        # Second SHA256 hash
        block_header_hash2 = hashlib.sha256(block_header_hash1).digest()

        # Compare block header hash to target
        return block_header_hash2[::-1] < self._target_comp[::-1]

    @asyncio.coroutine
    def submit_share(self, solution, share_response_future):
        yield from self._client.submit_share(solution, share_response_future)

    @property
    def difficulty(self):
        return self._difficulty


class DummyClient:
    """DummyClient that accepts url dummy://<difficulty> and generates dummy
    work at the specified difficulty."""

    def __init__(self, loop, work_manager, event_dispatcher, url, username=None, password=None, period=15.0):
        """Create an instance of DummyClient.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.
            url (str): Dummy Pool URL in the form of dummy://<difficulty>.
            username (str or None): Optional username.
            password (str or None): Optional password.
            period (float): Period to generate dummy work at.

        Returns:
            DummyClient: Instance of DummyClient.

        Raises:
            ValueError: if pool URL is invalid.
            Exception: if work is unsupported by miner.

        """
        self._loop = loop
        self._work_manager = work_manager
        self._event_dispatcher = event_dispatcher
        self._period = period

        # Parse Dummy url
        self._difficulty = DummyClient.parse_url(url)

        # Choose a work class based on work manager's supported work
        supported_work = work_manager.supported_work()
        if minerhal.MinerSimpleWork in supported_work:
            self._DummyWork = DummySimpleWork
        elif minerhal.MinerBitshareWork in supported_work:
            self._DummyWork = DummyBitshareWork
        else:
            raise Exception("Unknown work supported by miner.")

        self._stop_event = asyncio.Event()
        self._task = loop.create_task(self.run())

    @staticmethod
    def parse_url(url):
        """Parse dummy pool URL and return the difficulty specified in the URL.

        Args:
            url (str): Dummy pool URL.

        Returns:
            float: Difficulty specified in the URL.

        Raises:
            ValueError: if pool URL is invalid.

        """
        parsed_url = urllib.parse.urlparse(url)

        if parsed_url.scheme != "dummy":
            raise ValueError("Dummy URL error: Only dummy scheme is supported.")

        try:
            difficulty = float(parsed_url.netloc)
        except ValueError:
            raise ValueError("Dummy URL error: Invalid difficulty.")

        return difficulty

    @asyncio.coroutine
    def _process_share(self, share_response_future):
        """Simulate processing a share at the pool and fulfill the share
        response future.

        Sleeps for a 0.5 second processing delay and then populates the
        share_response_future with a ClientShareResponse instance of good
        classification.

        Args:
            share_response_future (asyncio.Future): Future to populate with a
                ClientShareResponse instance.

        """
        yield from asyncio.sleep(0.5)
        share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.good, None))

    @asyncio.coroutine
    def submit_share(self, solution, share_response_future):
        """Submit a solution to the dummy pool.

        Args:
            solution (minerhal.MinerSolution): Miner solution.
            share_response_future (asyncio.Future): Future to populate with a
                ClientShareResponse instance.

        """
        # Simulate server side processing
        self._loop.call_soon(asyncio.async, self._process_share(share_response_future))

    @asyncio.coroutine
    def run(self):
        """Produce dummy work at the period specified in the class
        constructor."""
        stop_request = asyncio.Task(self._stop_event.wait())

        self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(True, "dummy://{}".format(self._difficulty), "none", "Connected to dummy."))
        while True:
            # Broadcast new work
            yield from self._work_manager.load_work(self._DummyWork(self, self._difficulty))

            # Wait for sleep or stop
            done, pending = yield from asyncio.wait([asyncio.sleep(self._period), stop_request], return_when=asyncio.FIRST_COMPLETED)
            if stop_request in done:
                break

        # Cancel pending sleep
        for task in pending:
            task.cancel()

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        self._stop_event.set()
        yield from self._task
