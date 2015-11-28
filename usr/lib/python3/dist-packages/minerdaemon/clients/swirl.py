import os
import asyncio
import logging
import collections
import urllib.parse
import codecs
import hashlib
import math
import ssl

import minerhal
import sha256
from . import utils

import google.protobuf.message
from . import swirl_pb3

import minerdaemon.events
from minerdaemon.workmanager import ClientWork, ClientShareResponse


logger = logging.getLogger('minerd')

ConnectionInfo = collections.namedtuple("ConnectionInfo", ['url', 'host', 'port', 'username', 'uuid', 'ssl_context'])
"""Container for connection information and credentials."""

SwirlWorkData = collections.namedtuple("SwirlWorkData", ['work_id', 'version', 'prev_block_hash', 'height', 'nbits', 'ntime', 'coinb1', 'coinb2', 'merkle_edge', 'new_block', 'bits_pool'])
"""Container for a Swirl work notification."""


class SwirlWork(ClientWork):
    """The SwirlWork implementation of the ClientWork interface."""

    _BITSHARE_OUTPUT_LEN = 38
    """Length of bitshare output appended by the ASIC. This suffix should be
    cut-off from the coinbase when computing its midstate to send to the
    ASIC."""

    def __init__(self, client, enonce1, enonce2_size, wallet_id, swirl_work_data, miner_work_type):
        """Create an instance of SwirlWork.

        Args:
            client (SwirlClient): Client associated with this work.
            enonce1 (bytes): enonce1.
            enonce2_size (int): Size of enonce2.
            wallet_id (int): ASIC wallet id.
            swirl_work_data (SwirlWorkData): Swirl work notification data.
            miner_work_type (type): minerhal.MinerSimpleWork or minerhal.MinerBitshareWork.

        Returns:
            SwirlWork: Instance of SwirlWork.

        """
        self._client = client
        self._enonce1 = enonce1
        self._enonce2_size = enonce2_size
        self._wallet_id = wallet_id
        self._swirl_work_data = swirl_work_data
        self._miner_work_type = miner_work_type

        # Calculate pool difficulty and target
        self._pool_difficulty = utils.bits_to_difficulty(swirl_work_data.bits_pool)
        self._target_comp = utils.bits_to_target(swirl_work_data.bits_pool).to_bytes(32, byteorder='little')

        self._derived_works = {}

    def derive_works(self, count):
        if self._miner_work_type == minerhal.MinerSimpleWork:
            return self._derive_simple_works(count)
        elif self._miner_work_type == minerhal.MinerBitshareWork:
            return self._derive_bitshare_works(count)

    def _derive_simple_works(self, count):
        works = []

        for _ in range(count):
            # Form enonce2
            enonce2 = os.urandom(self._enonce2_size)

            # Generate coinbase transaction
            coinbase_transaction = self._swirl_work_data.coinb1 + self._enonce1 + enonce2 + self._swirl_work_data.coinb2

            # Compute merkle root
            merkle_root = hashlib.sha256(hashlib.sha256(coinbase_transaction).digest()).digest()
            for node in self._swirl_work_data.merkle_edge:
                merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + node).digest()).digest()

            merkle_lsw = merkle_root[28:32]

            # Form first 64 bytes of block header
            block_header_first = self._swirl_work_data.version.to_bytes(4, 'little') + self._swirl_work_data.prev_block_hash + merkle_root[:28]

            # Calculate SHA256 midstate
            (midstate, _) = sha256.sha256(block_header_first).state

            work = minerhal.MinerSimpleWork(
                midstate=midstate,
                merkle_lsw=merkle_lsw,
                bits=self._swirl_work_data.nbits,
                timestamp=self._swirl_work_data.ntime,
                bits_comp=self._swirl_work_data.bits_pool
            )

            # Save the midstate under merkle_lsw for later validation, and
            # enonce2 for later share submission
            self._derived_works[merkle_lsw] = (midstate, enonce2, merkle_root, coinbase_transaction)
            # FIXME remove full merkle root and coinbase_transaction above when
            # logging is quieted.

            works.append(work)

        return works

    def _derive_bitshare_works(self, count):
        works = []

        # Block header miner structure
        block_header = minerhal.MinerBitshareWork.BlockHeader(
            version=self._swirl_work_data.version,
            prev_block_hash=self._swirl_work_data.prev_block_hash,
            bits=self._swirl_work_data.nbits,
            timestamp=self._swirl_work_data.ntime,
            bits_comp=self._swirl_work_data.bits_pool
        )

        # Merkle edge miner structure
        merkle_edge = minerhal.MinerBitshareWork.MerkleEdge(
            list(self._swirl_work_data.merkle_edge)
        )

        for _ in range(count):
            # Form enonce2
            enonce2 = os.urandom(self._enonce2_size)

            # Generate coinbase transaction
            coinbase_transaction = self._swirl_work_data.coinb1 + self._enonce1 + enonce2 + self._swirl_work_data.coinb2

            # Check if coinbase transaction is 512-bit aligned before the bitshare output
            if ((len(coinbase_transaction) - SwirlWork._BITSHARE_OUTPUT_LEN) % 64) != 0:
                logger.warning("[swirl_client] Received improperly padded coinbase transaction of length %d", len(coinbase_transaction))

            (coinbase_midstate, _) = sha256.sha256(coinbase_transaction[:-SwirlWork._BITSHARE_OUTPUT_LEN]).state

            # Coinbase miner structure
            coinbase = minerhal.MinerBitshareWork.Coinbase(
                midstate=coinbase_midstate,
                wallet_id=self._wallet_id,
                block_height=self._swirl_work_data.height,
                lock_time=int.from_bytes(coinbase_transaction[-4:], 'little'),
                data_length=8*len(coinbase_transaction)
            )

            # Compute merkle root
            merkle_root = hashlib.sha256(hashlib.sha256(coinbase_transaction).digest()).digest()
            for node in self._swirl_work_data.merkle_edge:
                merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + node).digest()).digest()

            merkle_lsw = merkle_root[28:32]

            # Form first 64 bytes of block header
            block_header_first = self._swirl_work_data.version.to_bytes(4, 'little') + self._swirl_work_data.prev_block_hash + merkle_root[:28]

            # Calculate SHA256 midstate
            (midstate, _) = sha256.sha256(block_header_first).state

            work = minerhal.MinerBitshareWork(
                block_header,
                merkle_edge,
                coinbase
            )

            # Save the midstate under merkle_lsw for later validation, and
            # enonce2 for later share submission
            self._derived_works[merkle_lsw] = (midstate, enonce2, merkle_root, coinbase_transaction)
            # FIXME remove full merkle root and coinbase_transaction above when
            # logging is quieted.

            works.append(work)

        return works

    def validate_solution(self, solution):
        # Validate solution parameters match loaded work
        if solution.merkle_lsw not in self._derived_works \
                or self._derived_works[solution.merkle_lsw][0] != solution.midstate \
                or self._swirl_work_data.nbits != solution.bits \
                or self._swirl_work_data.bits_pool != solution.bits_comp \
                or (self._swirl_work_data.ntime & 0xffffff00) != (solution.timestamp & 0xffffff00):
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

        logger.info("[swirl_client] Validating solution for work_id 0x%x with enonce1 %s, enonce2 %s, solution %s", self._swirl_work_data.work_id, codecs.encode(self._enonce1, "hex_codec"), codecs.encode(self._derived_works[solution.merkle_lsw][1], "hex_codec"), str(solution))
        logger.info("[swirl_client] coinbase dhash %s", codecs.encode(hashlib.sha256(hashlib.sha256(self._derived_works[solution.merkle_lsw][3]).digest()).digest(), "hex_codec"))
        for node in self._swirl_work_data.merkle_edge:
            logger.info("[swirl_client] edge     dhash %s", codecs.encode(node, "hex_codec"))
        logger.info("[swirl_client] Block header")
        logger.info("[swirl_client]     version 0x%08x", self._swirl_work_data.version)
        logger.info("[swirl_client]     prev block hash %s", codecs.encode(self._swirl_work_data.prev_block_hash, "hex_codec"))
        logger.info("[swirl_client]     merkle root %s", codecs.encode(self._derived_works[solution.merkle_lsw][2], "hex_codec"))
        logger.info("[swirl_client]     timestamp 0x%08x", solution.timestamp)
        logger.info("[swirl_client]     bits 0x%08x", solution.bits)
        logger.info("[swirl_client]     nonce 0x%08x", solution.nonce)
        logger.info("[swirl_client] Block header hash %s", codecs.encode(block_header_hash2, "hex_codec"))

        # Compare block header hash to target
        return block_header_hash2[::-1] < self._target_comp[::-1]

    @asyncio.coroutine
    def submit_share(self, solution, share_response_future):
        # Gather necessary fields for share submission
        work_id = self._swirl_work_data.work_id
        enonce2 = self._derived_works[solution.merkle_lsw][1]
        timestamp = solution.timestamp
        nonce = solution.nonce

        yield from self._client.submit_share(work_id, enonce2, timestamp, nonce, share_response_future)

    @property
    def difficulty(self):
        return self._pool_difficulty


class SwirlClientProtocol(asyncio.Protocol):
    """Client connection context and handler class for Swirl."""

    REPLY_TIMEOUT = 10.0
    """Timeout in seconds for server reply to a request."""

    MAX_CONSECUTIVE_TIMEOUT_SHARES = 10
    """Maximum number of consecutive timeout share submissions before
    disconnection."""

    def __init__(self, loop, client, work_manager, event_dispatcher, connection_info, miner_work_type, connection_succeeded_event, connection_lost_event):
        """Create an instance of SwirlClientProtocol.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            client (SwirlClient): SwirlClient associated with this instance.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.
            connection_info (SwirlConnectionInfo): Connection information.
            miner_work_type (type): minerhal.MinerSimpleWork or minerhal.MinerBitshareWork.
            connection_succeeded_event (asyncio.Event): Event set for connection succeeded.
            connection_lost_event (asyncio.Event): Event set for connection lost.

        Returns:
            SwirlClientProtocol: Instance of SwirlClientProtocol.

        """
        self._loop = loop
        self._client = client
        self._work_manager = work_manager
        self._event_dispatcher = event_dispatcher
        self._connection_info = connection_info
        self._miner_work_type = miner_work_type
        self._connection_succeeded_event = connection_succeeded_event
        self._connection_lost_event = connection_lost_event

        self._client.connection = self

        # Transport populated in connection_made()
        self._transport = None
        # Authentication error
        self._error = None

        # Message ID counter
        self._next_message_id = 1

        # Reply futures
        self._auth_reply = None
        self._submit_share_replies = {}

        # Receive buffer
        self._buf = b""

        # Work related state
        self._authenticated = asyncio.Event()
        self._enonce1 = None
        self._enonce2_size = None
        self._wallet_id = None
        self._work_data = None
        self._consecutive_timeout_shares_count = 0

    def connection_made(self, transport):
        """asyncio.Protocol connection made callback. Starts the authentication
        task.

        Args:
            transport (asyncio.BaseTransport): Transport.

        """
        logger.debug("[swirl_client] Connection made!")
        self._transport = transport
        self._loop.create_task(self.authenticate())

    def connection_lost(self, exc):
        """asyncio.Protocol connection lost callback.

        Args:
            exc (Exception or None): Exception for connection loss.

        """
        if exc is not None:
            reason = str(exc)
        elif exc is None and self._error is not None:
            reason = self._error
        else:
            reason = "Server disconnected."

        logger.debug("[swirl_client] Connection lost: %s", str(reason))
        self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(False, self._connection_info.url, self._connection_info.username, "Connection lost: {}".format(reason)))

        # Idle work manager
        self._work_manager.idle()

        # Notify the reconnect loop that the connection was lost
        self._connection_lost_event.set()

    def close(self):
        """Close the connection."""
        self._error = "Client disconnected due to shutdown."
        if self._transport is not None:
            self._transport.close()

    def data_received(self, data):
        """asyncio.Protocol data received callback. Decodes JSON data into
        Swirl messages and dispatches them to the handle() coroutine.

        Args:
            data (bytes): Data received.

        """
        self._buf += data

        while True:
            # Check if buf is long enough for length field
            if len(self._buf) < 2:
                break

            # Read length field
            length = int.from_bytes(self._buf[0:2], 'big')

            # Check if buf is long enough for message
            if len(self._buf) < 2+length:
                break

            # Extract serialized message
            message_data = self._buf[2:2+length]
            self._buf = self._buf[2+length:]

            # Deserialize message
            message = swirl_pb3.SwirlServerMessage()
            try:
                message.ParseFromString(message_data)
            except google.protobuf.message.DecodeError as e:
                logger.debug("[swirl_client] Error decoding server message: %s", str(e))
                return

            self._loop.create_task(self.handle(message))

    @asyncio.coroutine
    def authenticate(self):
        """Authenticate with the server."""
        # Build AuthRequest message
        client_message = swirl_pb3.SwirlClientMessage()
        if self._miner_work_type == minerhal.MinerBitshareWork:
            client_message.auth_request.hardware = swirl_pb3.SwirlClientMessage.AuthRequest.bitshare
        else:
            client_message.auth_request.hardware = swirl_pb3.SwirlClientMessage.AuthRequest.generic
        client_message.auth_request.username = self._connection_info.username
        client_message.auth_request.uuid = self._connection_info.uuid

        # Create future for the reply
        self._auth_reply = asyncio.Future()

        logger.debug("[swirl_client] Sending auth request: %s", str(client_message))

        # Serialize and send AuthRequest message
        message_data = client_message.SerializeToString()
        self._transport.write(len(message_data).to_bytes(2, 'big') + message_data)

        # Wait for auth reply or timeout
        try:
            server_message = yield from asyncio.wait_for(self._auth_reply, timeout=self.REPLY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("[swirl_client] Authorization reply timed out.")
            self._auth_reply = None
            self._error = "Authorize: timed out."
            self._transport.close()
            return

        auth_reply_type = server_message.auth_reply.WhichOneof("authreplies")

        # Handle auth replies
        if auth_reply_type == "auth_reply_no":
            self._error = "Authorize: Error: {}".format(server_message.auth_reply.auth_reply_no.error)
            self._transport.close()
            return
        elif auth_reply_type == "auth_reply_pool_down":
            self._error = "Authorize: Pool down: {}".format(server_message.auth_reply.auth_reply_pool_down.reason)
            # FIXME respect auth_reply.retry_seconds in reconnect
            self._transport.close()
            return
        elif auth_reply_type != "auth_reply_yes":
            self._error = "Authorize: Unknown error."
            self._transport.close()
            return

        # Save enonce1 and enonce2_size from AuthReply
        self._enonce1 = server_message.auth_reply.auth_reply_yes.enonce1
        self._enonce2_size = server_message.auth_reply.auth_reply_yes.enonce2_size
        self._wallet_id = server_message.auth_reply.auth_reply_yes.wallet_id

        logger.debug("[swirl_client] Authorization success. enonce1: %s enonce2_size: %d", self._enonce1, self._enonce2_size)

        self._authenticated.set()
        self._connection_succeeded_event.set()

        self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(True, self._connection_info.url, self._connection_info.username, "Connected to pool."))

        # Load work now, if we can
        yield from self.load_work()

    @asyncio.coroutine
    def handle(self, message):
        """Handle a Swirl server message.

        Args:
            message (swirl_pb3.SwirlServerMessage): Swirl server message.

        """
        message_type = message.WhichOneof("servermessages")

        if message_type == "auth_reply":
            # Fulfill auth reply future
            if self._auth_reply is not None:
                self._auth_reply.set_result(message)
                return

            logger.debug("[swirl_client] Got spurious auth reply: %s", str(message))
        elif message_type == "submit_share_reply":
            # Fulfill submit share reply future
            if message.submit_share_reply.message_id in self._submit_share_replies:
                self._submit_share_replies[message.submit_share_reply.message_id].set_result(message)
                del self._submit_share_replies[message.submit_share_reply.message_id]
                return

            logger.debug("[swirl_client] Got submit reply to unknown ID: %s", str(message))
        elif message_type == "work_notification":
            # Extract work data from work notification
            self._work_data = SwirlWorkData(
                work_id=message.work_notification.work_id,
                version=message.work_notification.version,
                prev_block_hash=message.work_notification.prev_block_hash,
                height=message.work_notification.height,
                nbits=message.work_notification.nbits,
                ntime=message.work_notification.ntime,
                coinb1=message.work_notification.coinb1,
                coinb2=message.work_notification.coinb2,
                merkle_edge=message.work_notification.merkle_edge,
                new_block=message.work_notification.new_block,
                bits_pool=message.work_notification.bits_pool
            )
            logger.debug("[swirl_client] Got work notification.")

            yield from self.load_work()
        else:
            logger.debug("[swirl_client] Got unknown message: %s", str(message))

    @asyncio.coroutine
    def load_work(self):
        """Create a new SwirlWork object for the current work and load it on
        the work manager."""
        if self._authenticated.is_set() and self._work_data is not None:
            logger.debug("[swirl_client] Loading work: %s", str(self._work_data))
            yield from self._work_manager.load_work(SwirlWork(self, self._enonce1, self._enonce2_size, self._wallet_id, self._work_data, self._miner_work_type))

    @asyncio.coroutine
    def submit_share(self, work_id, enonce2, timestamp, nonce, share_response_future):
        """Submit a share to the server.

        Args:
            work_id (int): Swirl work id.
            enonce2 (bytes): Solution coinbase enonce2.
            timestamp (int): Solution timestamp.
            nonce (int): Solution nonce.
            share_response_future (asyncio.Future): Future to populate with a
                ClientShareResponse instance.

        """
        # Make sure we're authenticated
        if not self._authenticated.is_set():
            try:
                yield from asyncio.wait_for(self._authenticated.wait(), timeout=self.REPLY_TIMEOUT)
            except asyncio.TimeoutError:
                logger.debug("[swirl_client] Wait for authentication before submitting share timeout.")
                share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.timeout, "Waiting for authentication timed out."))
                return

        # Generate a new message id
        message_id = self._next_message_id
        self._next_message_id += 1

        # Build SubmitShareRequest message
        client_message = swirl_pb3.SwirlClientMessage()
        client_message.submit_share_request.message_id = message_id
        client_message.submit_share_request.work_id = work_id
        client_message.submit_share_request.enonce2 = enonce2
        client_message.submit_share_request.otime = timestamp
        client_message.submit_share_request.nonce = nonce

        logger.debug("[swirl_client] Sending submit share request: %s", str(client_message))

        # Serialize and send message
        message_data = client_message.SerializeToString()
        self._transport.write(len(message_data).to_bytes(2, 'big') + message_data)

        # Create and store reply future
        self._submit_share_replies[message_id] = asyncio.Future()

        # Create a task to wait for reply future
        self._loop.create_task(self._handle_share_response(work_id, message_id, share_response_future))

    @asyncio.coroutine
    def _handle_share_response(self, work_id, message_id, share_response_future):
        """Wait for a share response or timeout and populate the share response
        future with the classification.

        Args:
            work_id (int): Swirl work id.
            message_id (int): Swirl response message id.
            share_response_future (asyncio.Future): Future to populate with a
                ClientShareResponse instance.

        """
        # Wait for submit share reply or timeout
        try:
            reply = yield from asyncio.wait_for(self._submit_share_replies[message_id], timeout=self.REPLY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("[swirl_client] Submit share reply for work id %s timed out.", str(work_id))
            del self._submit_share_replies[message_id]

            # Produce timeout ClientShareResponse
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.timeout, "Server reply timed out."))

            # Track consecutive timeouts and trigger disconnect on threshold
            self._consecutive_timeout_shares_count += 1
            if self._consecutive_timeout_shares_count >= self.MAX_CONSECUTIVE_TIMEOUT_SHARES:
                self._error = "Client disconnected. Reached {} consecutive share timeouts.".format(self._consecutive_timeout_shares_count)
                self._transport.close()

            return

        # Reset consecutive timeout shares counter
        self._consecutive_timeout_shares_count = 0

        logger.debug("[swirl_client] Got submit share reply: %s", str(reply))

        # Produce ClientShareResponse from submit response
        if reply.submit_share_reply.submit_status == swirl_pb3.SwirlServerMessage.SubmitShareReply.good:
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.good))
        elif reply.submit_share_reply.submit_status == swirl_pb3.SwirlServerMessage.SubmitShareReply.bad:
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.invalid))
        elif reply.submit_share_reply.submit_status == swirl_pb3.SwirlServerMessage.SubmitShareReply.stale:
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.stale))
        elif reply.submit_share_reply.submit_status == swirl_pb3.SwirlServerMessage.SubmitShareReply.duplicate:
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.duplicate))
        else:
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.other))


class SwirlClient:
    """SwirlClient that accepts url swirl+tcp://<host>:<port> or swirl+ssl://<host>:<port>."""

    RECONNECT_TIMEOUT = 1.0
    """Initial reconnect timeout."""

    MAX_RECONNECT_TIMEOUT = 512.0
    """Maximum reconnect timeout."""

    def __init__(self, loop, work_manager, event_dispatcher, url, username, password=None, uuid=""):
        """Create an instance of SwirlClient.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.
            url (str): Swirl pool URL in the form of swirl+tcp://<host>:<port> or swirl+ssl://<host>:<port>.
            username (str): Username.
            password (str or None): Optional password.
            uuid (str): Optional uuid.

        Returns:
            SwirlClient: Instance of SwirlClient.

        Raises:
            ValueError: if Swirl pool URL is invalid.
            Exception: if work is unsupported by miner.

        """
        self._loop = loop
        self._work_manager = work_manager
        self._event_dispatcher = event_dispatcher
        self._miner_work_type = None

        # Choose a work type based on work manager's supported work
        supported_work = work_manager.supported_work()
        if minerhal.MinerSimpleWork in supported_work:
            self._miner_work_type = minerhal.MinerSimpleWork
        elif minerhal.MinerBitshareWork in supported_work:
            self._miner_work_type = minerhal.MinerBitshareWork
        else:
            raise ValueError("Unknown work supported by miner.")

        # Parse Swirl url
        (host, port, ssl_context) = SwirlClient.parse_url(url)

        self._connection_info = ConnectionInfo(
            url=url,
            host=host,
            port=port,
            username=username,
            uuid=uuid,
            ssl_context=ssl_context
        )

        self.connection = None
        self._stop_event = asyncio.Event()

        self._task = self._loop.create_task(self.run())

    @staticmethod
    def parse_url(url):
        """Parse Swirl pool URL and return the host, port, and SSL context.

        Args:
            url (str): Swirl pool URL.

        Returns:
            tuple: pool host (str), port (int), and SSL context (ssl.SSLContext).

        Raises:
            ValueError: if pool URL is invalid.

        """
        parsed_url = urllib.parse.urlparse(url)
        netloc_split = parsed_url.netloc.split(":")

        if parsed_url.scheme not in ["swirl+tcp", "swirl+ssl"]:
            raise ValueError("Swirl URL error: Only swirl+tcp and swirl+ssl schemes are supported.")

        if len(netloc_split) < 2:
            raise ValueError("Swirl URL error: Missing port.")

        if len(netloc_split) != 2:
            raise ValueError("Swirl URL error: Malformed network location.")

        host = netloc_split[0]

        try:
            port = int(netloc_split[1])
        except ValueError:
            raise ValueError("Swirl URL error: Invalid port.")

        if parsed_url.scheme == "swirl+ssl":
            ssl_context = ssl.create_default_context()
        else:
            ssl_context = None

        return (host, port, ssl_context)

    @asyncio.coroutine
    def run(self):
        """Task that maintains a connection to the Swirl pool server,
        including the initial connection and subsequent reconnections on
        connection loss."""
        stop_request = asyncio.Task(self._stop_event.wait())

        failed_connections_count = 0
        max_failed_connections_count = math.ceil(math.log(self.MAX_RECONNECT_TIMEOUT, 2))

        while not self._stop_event.is_set():
            # Sleep between failed connection attempts
            if failed_connections_count > 0:
                # Wait for sleep or stop
                done, pending = yield from asyncio.wait([asyncio.sleep(self.RECONNECT_TIMEOUT * (2 ** failed_connections_count)), stop_request], return_when=asyncio.FIRST_COMPLETED)
                if stop_request in done:
                    # Cancel pending sleep
                    for task in pending:
                        task.cancel()
                    break

            connection_succeeded_event = asyncio.Event()
            connection_lost_event = asyncio.Event()

            # Start connection
            try:
                yield from self._loop.create_connection(
                    lambda: SwirlClientProtocol(
                        self._loop,
                        self,
                        self._work_manager,
                        self._event_dispatcher,
                        self._connection_info,
                        self._miner_work_type,
                        connection_succeeded_event,
                        connection_lost_event,
                    ),
                    self._connection_info.host,
                    self._connection_info.port,
                    ssl=self._connection_info.ssl_context
                )
            except OSError as e:
                logger.debug("[swirl_client] Connection refused: %s", str(e))
                logger.debug("[swirl_client] Reconnecting...")
                self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(False, self._connection_info.url, self._connection_info.username, "Connection refused: {}. Reconnecting...".format(str(e))))
                failed_connections_count = min(failed_connections_count+1, max_failed_connections_count)
                continue

            # Wait for connection loss
            yield from connection_lost_event.wait()

            # If this connection succeeded, reset failed connections count,
            # otherwise increment it
            if connection_succeeded_event.is_set():
                failed_connections_count = 0
            else:
                failed_connections_count = min(failed_connections_count+1, max_failed_connections_count)

    @asyncio.coroutine
    def stop_and_join(self):
        """Stop and join this task."""
        # Set stop event to prevent reconnection
        self._stop_event.set()

        # Close active connection
        if self.connection is not None:
            self.connection.close()

        yield from self._task
