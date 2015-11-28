import os
import asyncio
import json
import codecs
import logging
import collections
import urllib.parse
import hashlib
import math

import minerhal
import sha256
from . import utils

import minerdaemon.events
from minerdaemon.workmanager import ClientWork, ClientShareResponse


logger = logging.getLogger('minerd')

ConnectionInfo = collections.namedtuple("ConnectionInfo", ['url', 'host', 'port', 'username', 'password'])
"""Container for connection information and credentials."""

StratumWorkData = collections.namedtuple("StratumWorkData", ['job_id', 'prev_block_hash', 'coinb1', 'coinb2', 'merkle_edge', 'version', 'bits', 'timestamp', 'clean_jobs'])
"""Container for a Stratum work notification."""


class StratumWork(ClientWork):
    """The StratumClient implementation of the ClientWork interface for
    deriving MinerSimpleWork."""

    def __init__(self, client, pool_difficulty, enonce1, enonce2_size, stratum_work_data):
        """Create an instance of StratumWork.

        Args:
            client (StratumClient): Client associated with this work.
            pool_difficulty (float): Pool difficulty to solve for.
            enonce1 (bytes): enonce1.
            enonce2_size (int): Size of enonce2.
            stratum_work_data (StratumWorkData): Stratum work notification data.

        Returns:
            StratumWork: Instance of StratumWork.

        """
        self._client = client
        self._pool_difficulty = pool_difficulty
        self._enonce1 = enonce1
        self._enonce2_size = enonce2_size
        self._stratum_work_data = stratum_work_data

        # Calculate pool bits and target
        self._bits_comp = utils.difficulty_to_bits(pool_difficulty)
        self._target_comp = utils.bits_to_target(self._bits_comp).to_bytes(32, byteorder='little')

        # Convert block header parts into integers
        self._bits = int.from_bytes(codecs.decode(self._stratum_work_data.bits, 'hex_codec'), 'big')
        self._timestamp = int.from_bytes(codecs.decode(self._stratum_work_data.timestamp, 'hex_codec'), 'big')

        self._derived_works = {}

    def derive_works(self, count):
        # Convert merkle_edge into bytes
        merkle_edge = [codecs.decode(node, 'hex_codec') for node in self._stratum_work_data.merkle_edge]

        # Convert coinbase transaction parts into bytes
        coinb1 = codecs.decode(self._stratum_work_data.coinb1, 'hex_codec')
        enonce1 = codecs.decode(self._enonce1, 'hex_codec')
        coinb2 = codecs.decode(self._stratum_work_data.coinb2, 'hex_codec')

        # Convert block header parts into bytes
        prev_block_hash = codecs.decode(self._stratum_work_data.prev_block_hash, 'hex_codec')
        prev_block_hash = b''.join([prev_block_hash[i*4:(i+1)*4][::-1] for i in range(0, 8)]) # Insanity
        # ^ Whoever invented Stratum did not understand how to pack the block
        # header, and decided to assemble everything in 32-bit big endian
        # chunks and then reverse every four bytes of the block header.
        # Therefore, the prev_block_hash arrives over Stratum with every four
        # bytes pre-reversed, so the later reverse puts it into internal byte
        # order.
        version = codecs.decode(self._stratum_work_data.version, 'hex_codec')[::-1]

        works = []

        for _ in range(count):
            # Form enonce2
            enonce2 = os.urandom(self._enonce2_size)

            # Generate coinbase transaction
            coinbase_transaction = coinb1 + enonce1 + enonce2 + coinb2

            # Compute merkle root
            merkle_root = hashlib.sha256(hashlib.sha256(coinbase_transaction).digest()).digest()
            for node in merkle_edge:
                merkle_root = hashlib.sha256(hashlib.sha256(merkle_root + node).digest()).digest()

            merkle_lsw = merkle_root[28:32]

            # Form first 64 bytes of block header
            block_header_first = version + prev_block_hash + merkle_root[:28]

            # Calculate SHA256 midstate
            (midstate, _) = sha256.sha256(block_header_first).state

            work = minerhal.MinerSimpleWork(
                midstate=midstate,
                merkle_lsw=merkle_lsw,
                bits=self._bits,
                timestamp=self._timestamp,
                bits_comp=self._bits_comp
            )

            # Save the midstate under merkle_lsw for later validation, and
            # enonce2 for later share submission
            self._derived_works[merkle_lsw] = (midstate, enonce2)

            works.append(work)

        return works

    def validate_solution(self, solution):
        # Validate solution parameters match loaded work
        if solution.merkle_lsw not in self._derived_works \
                or self._derived_works[solution.merkle_lsw][0] != solution.midstate \
                or self._bits != solution.bits \
                or self._bits_comp != solution.bits_comp \
                or (self._timestamp & 0xfffffff0) != (solution.timestamp & 0xfffffff0):
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
        # Encode necessary fields into ASCII hex for share submission
        job_id = self._stratum_work_data.job_id
        enonce2 = codecs.encode(self._derived_works[solution.merkle_lsw][1], 'hex_codec').decode()
        timestamp = codecs.encode(solution.timestamp.to_bytes(4, 'big'), 'hex_codec').decode()
        nonce = codecs.encode(solution.nonce.to_bytes(4, 'big'), 'hex_codec').decode()

        yield from self._client.submit_share(job_id, enonce2, timestamp, nonce, share_response_future)

    @property
    def difficulty(self):
        return self._pool_difficulty


class StratumClientProtocol(asyncio.Protocol):
    """Client connection context and handler class for Stratum."""

    REPLY_TIMEOUT = 10.0
    """Timeout in seconds for server reply to a request."""

    MAX_CONSECUTIVE_TIMEOUT_SHARES = 10
    """Maximum number of consecutive timeout share submissions before
    disconnection."""

    StratumRequest = collections.namedtuple("StratumRequest", ['id', 'method', 'params'])
    """Container for a Stratum protocol request."""

    StratumResponse = collections.namedtuple("StratumResponse", ['id', 'result', 'error'])
    """Container for a Stratum protocol response to a request."""

    StratumNotification = collections.namedtuple("StratumNotification", ['id', 'method', 'params'])
    """Container for a Stratum protocol notification."""

    _STRATUM_ERROR_CODE_TO_CLASSIFICATION = {
        # Official Stratum error codes

        # 20 - Other/Unknown
        20: ClientShareResponse.Classification.other,
        # 21 - Job not found (stale)
        21: ClientShareResponse.Classification.stale,
        # 22 - Duplicate share
        22: ClientShareResponse.Classification.duplicate,
        # 23 - Low difficulty share
        23: ClientShareResponse.Classification.invalid,
        # 24 - Unauthorized worker
        24: ClientShareResponse.Classification.unauthorized,
        # 25 - Not subscribed
        25: ClientShareResponse.Classification.other,

        # Unofficial Stratum error codes

        # 1 - Invalid Job ID
        1: ClientShareResponse.Classification.other,
        # 2 - Stale
        2: ClientShareResponse.Classification.stale,
        # 3 - Ntime out of range
        3: ClientShareResponse.Classification.other,
        # 4 - Duplicate
        4: ClientShareResponse.Classification.duplicate,
        # 5 - Above target
        5: ClientShareResponse.Classification.invalid,
    }
    """Share submit response error code to ClientShareResponse Classification
    map."""

    def __init__(self, loop, client, work_manager, event_dispatcher, connection_info, connection_succeeded_event, connection_lost_event):
        """Create an instance of StratumClientProtocol.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            client (StratumClient): StratumClient associated with this instance.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.
            connection_info (StratumConnectionInfo): Connection information.
            connection_succeeded_event (asyncio.Event): Event set for connection succeeded.
            connection_lost_event (asyncio.Event): Event set for connection lost.

        Returns:
            StratumClientProtocol: Instance of StratumClientProtocol.

        """
        self._loop = loop
        self._client = client
        self._work_manager = work_manager
        self._event_dispatcher = event_dispatcher
        self._connection_info = connection_info
        self._connection_succeeded_event = connection_succeeded_event
        self._connection_lost_event = connection_lost_event

        self._client.connection = self

        # Transport populated in connection_made()
        self._transport = None
        # Authentication error
        self._error = None

        # Message ID counter
        self._next_message_id = 1

        # Response table
        self._future_response_table = {}

        # Receive buffer
        self._buf = b""

        # Work related state
        self._authenticated = asyncio.Event()
        self._enonce1 = None
        self._enonce2_size = None
        self._pool_difficulty = None
        self._work_data = None
        self._consecutive_timeout_shares_count = 0

    # Encode a stratum message to json
    @staticmethod
    def stratum_encode_message(message):
        """Encode a Stratum Request, Response, or Notification protocol message
        to JSON.

        Args:
            message (StratumClientProtocol.StratumRequest,
                StratumClientProtocol.StratumResponse,
                StratumClientProtocol.StratumNotification): message

        Returns:
            str: Protocol message serialized to JSON.

        Raises:
            ValueError: if the message type is unknown.

        """
        if isinstance(message, StratumClientProtocol.StratumRequest):
            return json.dumps({"id": message.id, "method": message.method, "params": message.params}).encode()
        elif isinstance(message, StratumClientProtocol.StratumResponse):
            return json.dumps({"id": message.id, "error": message.method, "result": message.params}).encode()
        elif isinstance(message, StratumClientProtocol.StratumNotification):
            return json.dumps({"id": message.id, "method": message.method, "params": message.params}).encode()
        else:
            raise ValueError("Unknown Stratum message type.")

    # Decode json to a stratum message
    @staticmethod
    def stratum_decode_message(data):
        """Decode JSON to a Stratum Request, Response, or Notificiation
        protocol message.

        Args:
            data (str): JSON-serialized message.

        Returns:
            StratumClientProtocol.StratumRequest or
                StratumClientProtocol.StratumResponse or
                StratumClientProtocol.StratumNotification: Decoded message.

        Raises:
            ValueError: if the message is malformed.

        """
        try:
            obj = json.loads(data)
            if 'result' in obj or 'error' in obj:
                return StratumClientProtocol.StratumResponse(obj['id'], obj['result'], obj['error'])
            elif obj['id'] is not None:
                return StratumClientProtocol.StratumRequest(obj['id'], obj['method'], obj['params'])
            else:
                return StratumClientProtocol.StratumNotification(obj['id'], obj['method'], obj['params'])
        except (KeyError, IndexError):
            raise ValueError("Malformed Stratum message.")

    def close(self):
        """Close the connection."""
        self._error = "Client disconnected due to shutdown."
        if self._transport is not None:
            self._transport.close()

    def connection_made(self, transport):
        """asyncio.Protocol connection made callback. Starts the authentication
        task.

        Args:
            transport (asyncio.BaseTransport): Transport.

        """
        logger.debug("[stratum_client] Connection made!")
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

        logger.debug("[stratum_client] Connection lost: %s", str(reason))
        self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(False, self._connection_info.url, self._connection_info.username, "Connection lost: {}".format(reason)))

        # Idle work manager
        self._work_manager.idle()

        # Notify the reconnect loop that the connection was lost
        self._connection_lost_event.set()

    def data_received(self, data):
        """asyncio.Protocol data received callback. Decodes JSON data into
        Stratum messages and dispatches them to the handle() coroutine.

        Args:
            data (bytes): Data received.

        """
        self._buf += data

        # Find \n delimited JSON messages
        while b"\n" in self._buf:
            # Extract the message from _buf
            pos = self._buf.find(b"\n")
            message_data = self._buf[0:pos].decode()
            self._buf = self._buf[pos + 1:]

            # Attempt to decode the message
            try:
                message = StratumClientProtocol.stratum_decode_message(message_data)
            except ValueError:
                logger.debug("[stratum_client] Error decoding server message: %s", str(data))
                continue

            # Dispatch message to handle() coroutine
            self._loop.create_task(self.handle(message))

    def send_request(self, method, *params):
        """Send a Stratum request and return a future for its response.

        Args:
            method (str): Method name.
            params (tuple): Method parameters.

        Returns:
            tuple: response <asyncio.Future()> and message id <int>.

        """
        # Generate a new message id
        message_id = self._next_message_id
        self._next_message_id += 1

        # Create the request
        message = StratumClientProtocol.StratumRequest(message_id, method, params)

        logger.debug("[stratum_client] Sending request: %s", str(message))

        # Send the request
        self._transport.write(StratumClientProtocol.stratum_encode_message(message) + b"\n")

        # Create and add response future
        response = asyncio.Future()
        self._future_response_table[message_id] = response

        return response, message_id

    @asyncio.coroutine
    def authenticate(self):
        """Authenticate with the server."""
        # Subscribe
        response_future, message_id = self.send_request("mining.subscribe")

        # Wait for subscribe reply or timeout
        try:
            response = yield from asyncio.wait_for(response_future, timeout=self.REPLY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("[startum_client] Subscribe response timed out.")
            del self._future_response_table[message_id]
            self._error = "Subscribe: timed out."
            self._transport.close()
            return

        if response.error is not None:
            self._error = "miner.subscribe: Error: {}".format(response.error)
            self._transport.close()
            return
        if not isinstance(response.result, list) or len(response.result) < 3:
            self._error = "miner.subscribe: Malformed response: {}".format(response)
            self._transport.close()
            return

        # Save enonce1 and enonce2_size from mining.subscribe response
        self._enonce1 = response.result[1]
        self._enonce2_size = response.result[2]

        logger.debug("[stratum_client] Subscribe success. enonce1: %s enonce2_size: %d", self._enonce1, self._enonce2_size)

        # Authorize
        response_future, message_id = self.send_request("mining.authorize", self._connection_info.username, self._connection_info.password)

        # Wait for authorize reply or timeout
        try:
            response = yield from asyncio.wait_for(response_future, timeout=self.REPLY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("[startum_client] Authorize response timed out.")
            del self._future_response_table[message_id]
            self._error = "Authorize: timed out."
            self._transport.close()
            return

        if response.error is not None:
            self._error = "miner.authorize: Error: {}".format(response.error)
            self._transport.close()
            return
        if response.result != True:
            self._error = "miner.authorize: Authorization failed."
            self._transport.close()
            return

        logger.debug("[stratum_client] Authorization success.")

        self._authenticated.set()
        self._connection_succeeded_event.set()

        self._event_dispatcher.dispatch(minerdaemon.events.NetworkEvent(True, self._connection_info.url, self._connection_info.username, "Connected to pool."))

        # Load work now, if we can
        yield from self.load_work()

    @asyncio.coroutine
    def handle(self, message):
        """Handle a Stratum Response or Notification protocol message.

        Args:
            message (StratumClientProtocol.StratumResponse or
                StratumClientProtocol.StratumNotification): Stratum protocol
                message.

        """
        # Dispatch it to the correct handler
        if isinstance(message, StratumClientProtocol.StratumResponse):
            yield from self.handle_message_response(message)
        elif isinstance(message, StratumClientProtocol.StratumNotification):
            yield from self.handle_message_notification(message)

    @asyncio.coroutine
    def handle_message_response(self, message):
        """Handle a Stratum Response protocol message.

        Args:
            message (StratumClientProtocol.StratumResponse): Stratum protocol
                response message.

        """
        # Fulfill response future
        if message.id in self._future_response_table:
            self._future_response_table[message.id].set_result(message)
            del self._future_response_table[message.id]
        else:
            logger.debug("[stratum_client] Got response to unknown ID: %s", str(message))

    @asyncio.coroutine
    def handle_message_notification(self, message):
        """Handle a Stratum Notification protocol message.

        Args:
            message (StratumClientProtocol.StratumNotification): Stratum
                protocol notification message.

        """
        if message.method == "mining.set_difficulty":
            self._pool_difficulty = message.params[0]
            logger.debug("[stratum_client] Got difficulty %d.", self._pool_difficulty)
        elif message.method == "mining.notify":
            self._work_data = StratumWorkData(*message.params)
            logger.debug("[stratum_client] Got work notification.")
            yield from self.load_work()
        elif message.method == "client.show_message":
            logger.debug("[stratum_client] Show message: \"%s\".", str(message.params[0]))
        else:
            logger.debug("[stratum_client] Got unknown notification: %s", str(message))

    @asyncio.coroutine
    def load_work(self):
        """Create a new StratumWork object for the current work and load it on
        the work manager."""
        if self._authenticated.is_set() and self._pool_difficulty is not None:
            logger.debug("[stratum_client] Loading work: %s", str(self._work_data))
            yield from self._work_manager.load_work(StratumWork(self, self._pool_difficulty, self._enonce1, self._enonce2_size, self._work_data))

    @asyncio.coroutine
    def submit_share(self, job_id, enonce2, timestamp, nonce, share_response_future):
        """Submit a share to the server.

        Args:
            job_id (int): Stratum job id.
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
                logger.debug("[stratum_client] Wait for authentication before submitting share timeout.")
                share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.timeout, "Waiting for authentication timed out."))
                return

        # Submit request
        response_future, message_id = self.send_request("mining.submit", self._connection_info.username, job_id, enonce2, timestamp, nonce)

        # Create a task to wait for response future
        self._loop.create_task(self._handle_share_response(job_id, message_id, share_response_future))

    @asyncio.coroutine
    def _handle_share_response(self, job_id, message_id, share_response_future):
        """Wait for a share response or timeout and populate the share response
        future with the classification.

        Args:
            job_id (int): Stratum job id.
            message_id (int): Stratum response message id.
            share_response_future (asyncio.Future): Future to populate with a
                ClientShareResponse instance.

        """
        # Wait for response or timeout
        try:
            response = yield from asyncio.wait_for(self._future_response_table[message_id], timeout=self.REPLY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("[stratum_client] Submit share reply for job id %s timed out.", str(job_id))
            del self._future_response_table[message_id]

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

        logger.debug("[stratum_client] Got submit response: %s", str(response))

        # Produce ClientShareResponse from submit response
        if response.result == True:
            # Valid share
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.good))
        elif response.result == False and response.error is None:
            # Invalid share
            share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.invalid))
        else:
            # More specific error
            code = response.error[0] if len(response) > 0 else None
            msg = response.error[1] if len(response) > 1 else None

            # Translate Stratum error code to ClientShareResponse classification
            if code in StratumClientProtocol._STRATUM_ERROR_CODE_TO_CLASSIFICATION:
                share_response_future.set_result(ClientShareResponse(StratumClientProtocol._STRATUM_ERROR_CODE_TO_CLASSIFICATION[code], msg))
            else:
                share_response_future.set_result(ClientShareResponse(ClientShareResponse.Classification.other, msg))


class StratumClient:
    """StratumClient that accepts url stratum+tcp://<host>:<port>."""

    RECONNECT_TIMEOUT = 1.0
    """Initial reconnect timeout."""

    MAX_RECONNECT_TIMEOUT = 512.0
    """Maximum reconnect timeout."""

    def __init__(self, loop, work_manager, event_dispatcher, url, username, password):
        """Create an instance of StratumClient.

        Args:
            loop (asyncio.BaseEventLoop): Event loop.
            work_manager (WorkManager): Work manager.
            event_dispatcher (EventDispatcher): Event dispatcher.
            url (str): Stratum pool URL in the form of stratum+tcp://<host>:<port>.
            username (str): Username.
            password (str or None): Optional password.

        Returns:
            StratumClient: Instance of StratumClient.

        Raises:
            ValueError: if Stratum pool URL is invalid.
            Exception: if work is unsupported by miner.

        """
        self._loop = loop
        self._work_manager = work_manager
        self._event_dispatcher = event_dispatcher

        # Check work manager supports this work
        supported_work = work_manager.supported_work()
        if minerhal.MinerSimpleWork not in supported_work:
            raise Exception("Miner does not support work type (MinerSimpleWork) provided by StratumClient.")

        # Parse Stratum url
        (host, port) = StratumClient.parse_url(url)

        self._connection_info = ConnectionInfo(
            url=url,
            host=host,
            port=port,
            username=username,
            password=password
        )

        self.connection = None
        self._stop_event = asyncio.Event()

        self._task = self._loop.create_task(self.run())

    @staticmethod
    def parse_url(url):
        """Parse Stratum pool URL and return the host and port.

        Args:
            url (str): Stratum pool URL.

        Returns:
            tuple: pool host (str) and port (int).

        Raises:
            ValueError: if pool URL is invalid.

        """
        parsed_url = urllib.parse.urlparse(url)
        netloc_split = parsed_url.netloc.split(":")

        if parsed_url.scheme != "stratum+tcp":
            raise ValueError("Stratum URL error: Only stratum+tcp scheme is supported.")

        if len(netloc_split) < 2:
            raise ValueError("Stratum URL error: Missing port.")

        if len(netloc_split) != 2:
            raise ValueError("Stratum URL error: Malformed network location.")

        host = netloc_split[0]

        try:
            port = int(netloc_split[1])
        except ValueError:
            raise ValueError("Stratum URL error: Invalid port.")

        return (host, port)

    @asyncio.coroutine
    def run(self):
        """Task that maintains a connection to the Stratum pool server,
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
                    lambda: StratumClientProtocol(
                        self._loop,
                        self,
                        self._work_manager,
                        self._event_dispatcher,
                        self._connection_info,
                        connection_succeeded_event,
                        connection_lost_event,
                    ),
                    self._connection_info.host,
                    self._connection_info.port
                )
            except OSError as e:
                logger.debug("[stratum_client] Connection refused: %s", str(e))
                logger.debug("[stratum_client] Reconnecting...")
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
