#!/usr/bin/env python3
"""A minimal Dhole engine, written against docs/wire-contract.md alone.

This engine exists to keep one claim honest: an engine can be written in any
language.  It is deliberately NOT Go, shares no code with Dhole, and imports no
third-party package at all -- not even a NATS client or a protobuf runtime.  It
speaks the NATS text protocol over a socket and encodes protobuf by hand, in
about the amount of code the contract implies.

Anything Dhole's own Go engine does implicitly -- a shared constant, a helper,
an assumption nobody wrote down -- shows up here as something that had to be
guessed.  Everything that had to be guessed is marked GAP, and reported.

Run it as the conformance suite does:

    DHOLE_NATS_URL=nats://127.0.0.1:4222 \
    DHOLE_ENGINE_ID=my-engine DHOLE_ENGINE_TIER=trusted \
    DHOLE_BLOB_DIR=/var/lib/dhole/blobs python3 engine.py

`--ignore-cancel` makes it drop EngineControl{Cancel}.  It is not a feature: it
is the deliberately broken variant the conformance suite runs against itself to
prove its cancellation case can actually fail.
"""

import hashlib
import json
import os
import queue
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid

# ---------------------------------------------------------------------------
# protobuf, by hand
#
# Only two wire types are needed: varint (0) for numbers and enums, and
# length-delimited (2) for strings, bytes and nested messages.  Proto3 omits a
# field whose value is the zero value, which is why nothing here writes one.
# ---------------------------------------------------------------------------


def _varint(n):
    # A negative int32 travels as a TEN-byte varint: protobuf sign-extends it to
    # 64 bits rather than encoding a short negative number. Getting this wrong is
    # not a bad value on the wire -- the naive loop never terminates, because
    # shifting a negative Python int right converges on -1 and stays there. A
    # step killed by a signal reports exit_code -9, and that one value hangs an
    # engine that never tried it.
    if n < 0:
        n += 1 << 64
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(num, wire):
    return _varint((num << 3) | wire)


def f_varint(num, value):
    if not value:
        return b""
    return _tag(num, 0) + _varint(value)


def f_bytes(num, value):
    if not value:
        return b""
    if isinstance(value, str):
        value = value.encode("utf-8")
    return _tag(num, 2) + _varint(len(value)) + value


def f_msg(num, body):
    if not body:
        return b""
    return _tag(num, 2) + _varint(len(body)) + body


def f_packed(num, values):
    """A packed repeated scalar: how proto3 encodes `repeated uint32`."""
    if not values:
        return b""
    body = b"".join(_varint(v) for v in values)
    return _tag(num, 2) + _varint(len(body)) + body


def f_map(num, mapping):
    """map<string,string> is a repeated message of {1: key, 2: value}."""
    out = b""
    for k, v in mapping.items():
        out += f_msg(num, f_bytes(1, k) + f_bytes(2, v))
    return out


def parse(data):
    """Decode into {field_number: [value, ...]}; bytes for wire type 2."""
    fields = {}
    i, n = 0, len(data)
    while i < n:
        key, i = _read_varint(data, i)
        num, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _read_varint(data, i)
        elif wire == 2:
            length, i = _read_varint(data, i)
            value, i = data[i : i + length], i + length
        elif wire == 5:
            value, i = struct.unpack_from("<I", data, i)[0], i + 4
        elif wire == 1:
            value, i = struct.unpack_from("<Q", data, i)[0], i + 8
        else:
            raise ValueError("unsupported protobuf wire type %d" % wire)
        fields.setdefault(num, []).append(value)
    return fields


def _read_varint(data, i):
    shift, result = 0, 0
    while True:
        b = data[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def one(fields, num, default=None):
    values = fields.get(num)
    return values[-1] if values else default


def text(fields, num, default=""):
    value = one(fields, num)
    return value.decode("utf-8") if isinstance(value, bytes) else default


def repeated_varint(fields, num):
    """Accept a repeated scalar in either encoding: packed or one per tag."""
    out = []
    for value in fields.get(num, []):
        if isinstance(value, bytes):
            i = 0
            while i < len(value):
                v, i = _read_varint(value, i)
                out.append(v)
        else:
            out.append(value)
    return out


def decode_map(fields, num):
    out = {}
    for raw in fields.get(num, []):
        entry = parse(raw)
        out[text(entry, 1)] = text(entry, 2)
    return out


# ---------------------------------------------------------------------------
# The contract's constants.  Every one of these is spelled out in
# docs/dhole/v1/engine.proto or docs/wire-contract.md.
# ---------------------------------------------------------------------------

PHASE_ACCEPTED = 1
PHASE_SUCCEEDED = 3
PHASE_FAILED = 4
PHASE_CANCELLED = 5

STREAM_STDOUT = 1
STREAM_STDERR = 2

CAPABILITY_NETWORK = 1
CAPABILITY_SECRETS = 2

PROTOCOL_VERSIONS = [1]
HEARTBEAT_INTERVAL = 5.0
REGISTRATION_INTERVAL = 15.0


def caps_hash(capabilities):
    """The `<caps>` token of a dispatch subject.

    GAP: docs/wire-contract.md says only "a stable hash of the sorted
    capability set".  That is not implementable.  This is Dhole's actual
    algorithm, read out of internal/engine/registry_client.go: sha256 over the
    sorted, de-duplicated enum NUMBERS, each in decimal followed by a newline,
    truncated to the first 16 hex characters.  An engine that guesses any other
    hash subscribes to a subject nothing is published on and simply never
    receives work -- with no error anywhere.
    """
    digest = hashlib.sha256()
    for c in sorted(set(c for c in capabilities if c)):
        digest.update(("%d\n" % c).encode("ascii"))
    return digest.hexdigest()[:16]


def subsets(items):
    out = [[]]
    for item in items:
        out += [s + [item] for s in out]
    return out


# ---------------------------------------------------------------------------
# The NATS client: the text protocol, in one class.
# ---------------------------------------------------------------------------


class Nats:
    """CONNECT/SUB/PUB/MSG/PING/PONG, plus the JetStream API over request-reply."""

    def __init__(self, url, name):
        self.url = url
        self.name = name
        self.sock = None
        self.rfile = None
        self.write_lock = threading.Lock()
        self.subs = {}  # sid -> callback
        self.next_sid = 1
        self.sub_lock = threading.Lock()
        self.inbox_prefix = "_INBOX." + uuid.uuid4().hex
        self.inbox_seq = 0
        self.running = True

    def connect(self):
        host, port, user, password = self._parse(self.url)
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.settimeout(None)
        self.rfile = self.sock.makefile("rb")

        line = self.rfile.readline()
        if not line.startswith(b"INFO"):
            raise RuntimeError("expected INFO from the server, got %r" % line[:40])
        opts = {
            "verbose": False,
            "pedantic": False,
            "tls_required": False,
            "name": self.name,
            "lang": "python-minimal",
            "version": "0.1.0",
            "protocol": 1,
            "headers": True,
            "no_responders": False,
        }
        if user:
            opts["user"], opts["pass"] = user, password
        self._write(b"CONNECT " + json.dumps(opts).encode() + b"\r\n")
        self._write(b"PING\r\n")
        reply = self.rfile.readline()
        while reply.startswith(b"INFO") or reply.startswith(b"+OK"):
            reply = self.rfile.readline()
        if not reply.startswith(b"PONG"):
            raise RuntimeError("handshake failed: %r" % reply[:120])

        threading.Thread(
            target=self._read_loop, name="nats-reader", daemon=True
        ).start()

    @staticmethod
    def _parse(url):
        rest = url.split("://", 1)[-1]
        user = password = None
        if "@" in rest:
            creds, rest = rest.rsplit("@", 1)
            user, _, password = creds.partition(":")
        host, _, port = rest.partition(":")
        return host, int(port or 4222), user, password

    def _write(self, data):
        with self.write_lock:
            self.sock.sendall(data)

    def subscribe(self, subject, callback):
        """Subscribe, and route deliveries by SUBSCRIPTION ID, not by subject.

        This is the one place the NATS protocol will catch out anybody writing
        a client from scratch. A JetStream pull delivers the message under its
        ORIGINAL subject -- `job.dispatch.<tier>.<caps>` -- on the subscription
        that asked for it, not under the inbox the request named. Routing by
        subject therefore drops every dispatch on the floor while the server
        goes on believing the engine is working on it, which is precisely the
        failure mode nothing in any log explains.
        """
        with self.sub_lock:
            sid = self.next_sid
            self.next_sid += 1
            self.subs[sid] = callback
        self._write(("SUB %s %d\r\n" % (subject, sid)).encode())
        return sid

    def unsubscribe(self, sid):
        with self.sub_lock:
            self.subs.pop(sid, None)
        try:
            self._write(("UNSUB %d\r\n" % sid).encode())
        except Exception:  # noqa: BLE001
            pass

    def subscribe_queue(self, subject):
        """A subscription whose deliveries land in a Queue: one per consumer."""
        q = queue.Queue()
        sid = self.subscribe(
            subject, lambda subj, reply, data, status: q.put((data, reply, status))
        )
        return sid, q

    def publish(self, subject, payload, reply=""):
        head = ("PUB %s %s %d\r\n" % (subject, reply, len(payload))).encode()
        self._write(head + payload + b"\r\n")

    def new_inbox(self):
        with self.sub_lock:
            self.inbox_seq += 1
            return "%s.%d" % (self.inbox_prefix, self.inbox_seq)

    def request(self, subject, payload, timeout):
        inbox = self.new_inbox()
        sid, q = self.subscribe_queue(inbox)
        try:
            self.publish(subject, payload, reply=inbox)
            return q.get(timeout=timeout)
        finally:
            self.unsubscribe(sid)

    def _read_loop(self):
        try:
            while self.running:
                line = self.rfile.readline()
                if not line:
                    return
                op = line.split(b" ", 1)[0].upper()
                if op in (b"MSG", b"HMSG"):
                    self._deliver(op, line)
                elif op == b"PING":
                    self._write(b"PONG\r\n")
                elif op == b"-ERR":
                    log("nats error: %s" % line.decode(errors="replace").strip())
        except Exception as exc:  # noqa: BLE001
            if self.running:
                log("nats reader stopped: %r" % (exc,))

    def _deliver(self, op, line):
        parts = line.decode().split()
        subject, sid = parts[1], int(parts[2])
        status = ""
        if op == b"MSG":
            reply = parts[3] if len(parts) == 5 else ""
            total = int(parts[-1])
            body = self.rfile.read(total + 2)[:total]
        else:
            # HMSG carries NATS headers; JetStream reports "no messages" and
            # "request timeout" as a header-only message with a status code.
            reply = parts[3] if len(parts) == 6 else ""
            hdr_len, total = int(parts[-2]), int(parts[-1])
            raw = self.rfile.read(total + 2)[:total]
            status = raw[:hdr_len].decode(errors="replace")
            body = raw[hdr_len:]
        with self.sub_lock:
            callback = self.subs.get(sid)
        if callback:
            try:
                callback(subject, reply, body, status)
            except Exception as exc:  # noqa: BLE001
                log("handler for %s failed: %r" % (subject, exc))

    def close(self):
        self.running = False
        try:
            self.sock.close()
        except Exception:  # noqa: BLE001
            pass


def log(message):
    sys.stderr.write("[engine] %s\n" % message)
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# The engine
# ---------------------------------------------------------------------------


class Job:
    def __init__(self, dispatch, ack_subject):
        self.d = dispatch
        self.ack_subject = ack_subject
        self.proc = None
        self.cancelled = False
        self.timed_out = False
        self.lock = threading.Lock()


class Engine:
    def __init__(self, ignore_cancel=False):
        # GAP: the contract never says how an engine is configured -- how it
        # learns its bus URL, its identity, its tier, or where the object store
        # is.  These names are the conformance suite's convention.
        self.url = os.environ.get("DHOLE_NATS_URL", "nats://127.0.0.1:4222")
        self.engine_id = os.environ.get("DHOLE_ENGINE_ID", "minimal-python")
        self.tier = os.environ.get("DHOLE_ENGINE_TIER", "trusted")
        self.blob_dir = os.environ.get("DHOLE_BLOB_DIR", "/tmp/dhole-blobs")
        self.stream = os.environ.get("DHOLE_DISPATCH_STREAM", "DISPATCH")
        self.secret_subject = os.environ.get("DHOLE_SECRET_SUBJECT", "")
        self.slots = int(os.environ.get("DHOLE_ENGINE_SLOTS", "2"))
        self.capabilities = [CAPABILITY_NETWORK, CAPABILITY_SECRETS]
        self.ignore_cancel = ignore_cancel

        self.nats = Nats(self.url, self.engine_id)
        self.jobs = {}  # run/step/attempt -> Job
        self.jobs_lock = threading.Lock()
        self.free = threading.Semaphore(self.slots)
        self.running = True

    # -- lifecycle ---------------------------------------------------------

    def run(self):
        self.nats.connect()
        self.nats.subscribe("engine.control." + self.engine_id, self.on_control)
        self.register()
        threading.Thread(
            target=self.heartbeat_loop, name="heartbeat", daemon=True
        ).start()
        for caps in subsets(sorted(self.capabilities)):
            threading.Thread(
                target=self.pull_loop,
                args=(caps_hash(caps),),
                name="pull-%s" % caps_hash(caps),
                daemon=True,
            ).start()
        log("running: id=%s tier=%s slots=%d" % (self.engine_id, self.tier, self.slots))
        while self.running:
            time.sleep(0.5)

    def register(self):
        """EngineMessage{registration} -- framed, never bare.

        The frame is what tells the plane a registration from a heartbeat by
        its BYTES: a heartbeat decodes cleanly as a registration, so an engine
        that publishes a bare payload is on the compatibility path for engines
        one version behind and will stop working.
        """
        reg = (
            f_bytes(1, self.engine_id)
            + f_packed(2, PROTOCOL_VERSIONS)
            + f_packed(3, sorted(self.capabilities))
            + f_bytes(4, platform_os())
            + f_bytes(5, platform_arch())
            + f_varint(6, self.slots)
            + f_bytes(7, "process")
            + f_bytes(8, self.tier)
        )
        self.nats.publish("engine.registration", f_msg(100, reg))

    def heartbeat_loop(self):
        last_registration = time.time()
        while self.running:
            body = f_bytes(1, self.engine_id)
            with self.jobs_lock:
                held = list(self.jobs.values())
            for job in held:
                d = job.d
                body += f_msg(
                    2,
                    f_bytes(1, d["run_id"])
                    + f_bytes(2, d["step_id"])
                    + f_varint(3, d["attempt"])
                    + f_bytes(4, d["fence_token"]),
                )
            try:
                self.nats.publish(
                    "engine.heartbeat." + self.engine_id, f_msg(101, body)
                )
                if time.time() - last_registration >= REGISTRATION_INTERVAL:
                    self.register()
                    last_registration = time.time()
            except Exception as exc:  # noqa: BLE001
                log("heartbeat failed: %r" % (exc,))
            time.sleep(HEARTBEAT_INTERVAL)

    # -- work queue --------------------------------------------------------

    def pull_loop(self, chash):
        """One outstanding pull request per capability set, always.

        The engine's slot limit deliberately does NOT gate the request. Gating
        it did, once, and cost an afternoon: a pull request already outstanding
        when the loop stopped to wait for a slot still had its message
        delivered, into a queue nobody was reading. The step sat unacknowledged
        for the whole ack_wait with the engine idle beside it. The bound that
        belongs on the server is max_ack_pending; the slot bound belongs around
        the WORK, and that is where it now is.
        """
        subject = "job.dispatch.%s.%s" % (self.tier, chash)
        durable = "engines-%s-%s" % (self.tier, chash)
        inbox = self.nats.new_inbox()
        _, deliveries = self.nats.subscribe_queue(inbox)
        if not self.ensure_consumer(durable, subject):
            return
        next_subject = "$JS.API.CONSUMER.MSG.NEXT.%s.%s" % (self.stream, durable)
        while self.running:
            try:
                self.nats.publish(
                    next_subject,
                    json.dumps({"batch": 1, "expires": 2_000_000_000}).encode(),
                    reply=inbox,
                )
                data, reply, status = deliveries.get(timeout=5)
            except queue.Empty:
                continue
            except Exception as exc:  # noqa: BLE001
                log("pull on %s failed: %r" % (subject, exc))
                time.sleep(0.5)
                continue
            if (
                not reply
                or status.startswith("NATS/1.0 4")
                or status.startswith("NATS/1.0 5")
            ):
                # 404 no messages, 408 request expired, 409 consumer deleted.
                continue
            threading.Thread(target=self.work, args=(data, reply), daemon=True).start()

    def ensure_consumer(self, durable, subject):
        """Bind the durable pull consumer this engine takes work from.

        A dispatch subject is a WORK QUEUE: exactly one engine receives each
        message, and an unacknowledged one is redelivered.  The consumer is
        named for the tier and capability set, not for this engine, which is
        what makes two engines share the queue instead of each getting a copy.
        """
        body = json.dumps(
            {
                "stream_name": self.stream,
                "config": {
                    "durable_name": durable,
                    "filter_subject": subject,
                    "ack_policy": "explicit",
                    # Longer than any step this engine will run: an ack_wait that
                    # expires mid-step hands the work to somebody else while it is
                    # still running here.
                    "ack_wait": 120_000_000_000,
                    # At least as many as this engine has slots. Lower, and ONE
                    # job that stops progressing stalls the whole queue for its
                    # capability set: the server will not deliver a second message
                    # while the first is unacknowledged, and every later step looks
                    # like an engine that has gone deaf.
                    "max_ack_pending": max(self.slots, 1),
                },
            }
        ).encode()
        api = "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s" % (self.stream, durable)
        deadline = time.time() + 60
        while time.time() < deadline and self.running:
            try:
                data, _, _ = self.nats.request(api, body, timeout=5)
                answer = json.loads(data.decode())
                if "error" not in answer:
                    return True
                log("consumer %s refused: %s" % (durable, answer["error"]))
            except queue.Empty:
                pass
            except Exception as exc:  # noqa: BLE001
                log("consumer %s: %r" % (durable, exc))
            # The plane may not have created the stream yet; an engine that
            # exited on that would make start-up ordering load-bearing.
            time.sleep(1.0)
        return False

    # -- one attempt -------------------------------------------------------

    def work(self, data, ack_subject):
        try:
            d = decode_dispatch(data)
        except Exception as exc:  # noqa: BLE001
            # Nothing to report against, and redelivering bytes that will not
            # parse loops forever: drop exactly this one.
            log("undecodable JobDispatch: %r" % (exc,))
            self.nats.publish(ack_subject, b"")
            self.free.release()
            return

        job = Job(d, ack_subject)
        key = "%s/%s/%d" % (d["run_id"], d["step_id"], d["attempt"])
        # The delivery is unacknowledged from here, so tell the server it is
        # being worked on -- including while this job is only WAITING for a
        # free slot. An ack_wait that elapses hands the step to another engine
        # while this one still holds it.
        held = threading.Event()
        threading.Thread(target=self.keepalive, args=(job, held), daemon=True).start()
        self.free.acquire()
        try:
            status = self.run_job(job, key)
            # The ordering rule: the terminal status goes out BEFORE the ack.
            # Acking first would lose the step in silence if this process died
            # between the two.
            self.publish_status(d, status)
            self.nats.publish(ack_subject, b"")
        except Exception as exc:  # noqa: BLE001
            log("job %s failed inside the engine: %r" % (key, exc))
            self.publish_status(
                d, {"phase": PHASE_FAILED, "error": "engine failure: %r" % (exc,)}
            )
            self.nats.publish(ack_subject, b"")
        finally:
            with self.jobs_lock:
                self.jobs.pop(key, None)
            held.set()
            self.free.release()

    def run_job(self, job, key):
        d = job.d
        if d["protocol_version"] not in PROTOCOL_VERSIONS:
            # Never silence: silence is indistinguishable from a dead engine
            # and the step hangs until its lease expires.
            return {
                "phase": PHASE_FAILED,
                "error": "unsupported protocol version %d; this engine speaks %s"
                % (d["protocol_version"], PROTOCOL_VERSIONS),
            }
        if not d["command"]:
            return {"phase": PHASE_FAILED, "error": "dispatch carries no command"}

        with self.jobs_lock:
            self.jobs[key] = job
        self.publish_status(d, {"phase": PHASE_ACCEPTED})

        workdir = tempfile.mkdtemp(prefix="dhole-step-")
        os.mkdir(os.path.join(workdir, "inputs"))
        os.mkdir(os.path.join(workdir, "outputs"))
        try:
            return self.execute(job, workdir)
        finally:
            subprocess.call(["rm", "-rf", workdir])

    def execute(self, job, workdir):
        d = job.d
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": workdir,
            "TMPDIR": workdir,
        }
        env.update(d["env"])

        try:
            for ref in d["inputs"]:
                self.materialise(ref, workdir)
        except Exception as exc:  # noqa: BLE001
            return {"phase": PHASE_FAILED, "error": "materialising inputs: %r" % (exc,)}

        try:
            for secret in d["secrets"]:
                env[secret["name"]] = self.redeem(secret)
        except Exception as exc:  # noqa: BLE001
            # The refusal names the BINDING, never the handle or the value: a
            # status is durable and archived.
            return {
                "phase": PHASE_FAILED,
                "error": "redeeming a secret reference: %s" % exc,
            }

        timeout = float(d["env"].get("DHOLE_STEP_TIMEOUT_SECONDS", "0") or 0)
        spool = os.path.join(workdir, "step.log")
        seq = Counter()
        with open(spool, "wb") as sink:
            # start_new_session puts the step in its own process group, which
            # is the only way `sh -c "... sleep 20"` really dies when it is
            # cancelled: killing the shell alone leaves the sleep behind, still
            # holding the pipe this engine is reading.
            proc = subprocess.Popen(
                d["command"],
                cwd=workdir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            with job.lock:
                job.proc = proc
            pumps = [
                threading.Thread(
                    target=self.pump,
                    args=(d, proc.stdout, STREAM_STDOUT, sink, seq),
                    daemon=True,
                ),
                threading.Thread(
                    target=self.pump,
                    args=(d, proc.stderr, STREAM_STDERR, sink, seq),
                    daemon=True,
                ),
            ]
            for t in pumps:
                t.start()
            watchdog = None
            if timeout > 0:
                watchdog = threading.Timer(timeout, self.expire, args=(job,))
                watchdog.start()
            exit_code = proc.wait()
            if watchdog:
                watchdog.cancel()
            for t in pumps:
                t.join(timeout=30)

        # The authoritative log is COMPLETE in the store before any terminal
        # status names it.  A reader that acts on the status must never find a
        # half-written log.
        # GAP: the contract names JobDispatch.output_prefix and
        # JobStatus.log_key but defines no object store protocol at all.  A
        # directory is the conformance suite's convention.
        log_key = "%s/attempt-%d.log" % (d["output_prefix"], d["attempt"])
        self.store(log_key, open(spool, "rb").read())

        with job.lock:
            cancelled, timed_out = job.cancelled, job.timed_out
        if cancelled:
            return {
                "phase": PHASE_CANCELLED,
                "log_key": log_key,
                "error": "cancelled by EngineControl",
            }
        if timed_out:
            return {
                "phase": PHASE_FAILED,
                "log_key": log_key,
                "exit_code": exit_code,
                "error": "step timed out after %gs" % timeout,
            }
        if exit_code != 0:
            return {
                "phase": PHASE_FAILED,
                "log_key": log_key,
                "exit_code": exit_code,
                "error": "step exited %d" % exit_code,
            }
        try:
            outputs = self.collect(d, workdir)
        except Exception as exc:  # noqa: BLE001
            return {
                "phase": PHASE_FAILED,
                "log_key": log_key,
                "error": "collecting outputs: %s" % exc,
            }
        return {
            "phase": PHASE_SUCCEEDED,
            "log_key": log_key,
            "exit_code": 0,
            "outputs": outputs,
        }

    def pump(self, d, pipe, stream, sink, seq):
        """Both copies of the step's output, from one read.

        The LogChunks are the live copy: ephemeral, best-effort, for a GUI
        watching the run.  The spool becomes the authoritative object.  A
        failure to publish must never fail the step.
        """
        while True:
            block = pipe.read1(32768) if hasattr(pipe, "read1") else pipe.read(32768)
            if not block:
                return
            sink.write(block)
            sink.flush()
            chunk = (
                f_bytes(1, d["run_id"])
                + f_bytes(2, d["step_id"])
                + f_varint(3, seq.next())
                + f_bytes(4, block)
                + f_varint(5, stream)
                + f_varint(6, d["attempt"])
            )
            try:
                self.nats.publish("job.logs.%s.%s" % (d["run_id"], d["step_id"]), chunk)
            except Exception:  # noqa: BLE001
                pass

    def keepalive(self, job, done):
        """Tell the server this delivery is still being worked on.

        Without it a step that outlives the consumer's ack_wait is redelivered
        to another engine while this one is still running it -- and the two
        attempts then race, which is exactly what the fence exists to sort out
        afterwards rather than something to cause on purpose.
        """
        while not done.wait(timeout=10):
            try:
                self.nats.publish(job.ack_subject, b"+WPI")
            except Exception:  # noqa: BLE001
                return

    def expire(self, job):
        with job.lock:
            job.timed_out = True
        self.kill(job)

    def kill(self, job):
        with job.lock:
            proc = job.proc
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:  # noqa: BLE001
            proc.kill()

    def materialise(self, ref, workdir):
        """A step reads exactly what it declared, at inputs/<port>.

        GAP: ports are in the contract; where they live for a process-executed
        step is not.  inputs/<port> and outputs/<port> are the suite's layout.
        """
        if ref.get("key"):
            data = self.load(ref["key"])
        elif ref.get("digest"):
            data = self.load(
                "cas/%s/%s" % (ref["digest"][0] or "sha256", ref["digest"][1])
            )
        else:
            raise ValueError("input %r names neither a key nor a digest" % ref["port"])
        with open(os.path.join(workdir, "inputs", ref["port"]), "wb") as fh:
            fh.write(data)

    def collect(self, d, workdir):
        outputs = []
        for port in d["outputs"]:
            path = os.path.join(workdir, "outputs", port)
            if not os.path.exists(path):
                raise ValueError(
                    "the step declared output %r but wrote nothing at outputs/%s"
                    % (port, port)
                )
            with open(path, "rb") as fh:
                data = fh.read()
            key = "%s/outputs/%s" % (d["output_prefix"], port)
            self.store(key, data)
            outputs.append(
                {
                    "port": port,
                    "key": key,
                    "size": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        return outputs

    def redeem(self, secret):
        """Exchange a handle for a value, at the moment it is needed.

        The value is bound to the step's environment and goes nowhere else: not
        into a log, not into the object store, not into an error message.

        GAP: the contract says a handle is redeemed and single-use.  It does
        not say on what subject, with what message, or what a refusal looks
        like.  This is the conformance suite's convention.
        """
        if not self.secret_subject:
            raise ValueError(
                "no redemption endpoint configured for %r" % secret["name"]
            )
        data, _, _ = self.nats.request(
            self.secret_subject, secret["handle"].encode(), timeout=10
        )
        value = data.decode()
        if value.startswith("ERR "):
            raise ValueError("the handle for %r was refused" % secret["name"])
        return value

    def store(self, key, data):
        path = os.path.join(self.blob_dir, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)

    def load(self, key):
        with open(os.path.join(self.blob_dir, key), "rb") as fh:
            return fh.read()

    # -- inbound control ---------------------------------------------------

    def on_control(self, subject, reply, data, status):
        """EngineControl is the only message an engine receives besides work."""
        fields = parse(data)
        cancel = one(fields, 1)
        if cancel is None:
            return  # Drain and Attach are not implemented here.
        if self.ignore_cancel:
            log("--ignore-cancel: dropping a Cancel (the broken variant)")
            return
        c = parse(cancel)
        key = "%s/%s/%d" % (text(c, 1), text(c, 2), one(c, 3, 0))
        fence = text(c, 4)
        with self.jobs_lock:
            job = self.jobs.get(key)
        if job is None:
            return
        if fence != job.d["fence_token"]:
            # A Cancel under a fence this engine does not hold is from an
            # attempt that has been superseded, or from a plane that no longer
            # owns the step.  Acting on it would let a stale authority kill a
            # live attempt.
            log("refusing Cancel for %s: fence %r is not the fence held" % (key, fence))
            return
        with job.lock:
            job.cancelled = True
        self.kill(job)

    # -- status ------------------------------------------------------------

    def publish_status(self, d, status):
        """Every status echoes the fence token unchanged: it is what lets the
        plane discard the report of an attempt that has been superseded."""
        body = (
            f_bytes(1, d["run_id"])
            + f_bytes(2, d["step_id"])
            + f_varint(3, d["attempt"])
            + f_bytes(4, d["fence_token"])
            + f_varint(5, status["phase"])
            + f_varint(6, status.get("exit_code", 0))
        )
        for out in status.get("outputs", []):
            body += f_msg(
                7,
                f_bytes(1, out["port"])
                + f_msg(2, f_bytes(1, "sha256") + f_bytes(2, out["sha256"]))
                + f_bytes(3, out["key"])
                + f_varint(4, out["size"]),
            )
        body += f_bytes(8, status.get("error", "")) + f_bytes(
            9, status.get("log_key", "")
        )
        self.nats.publish("job.status.%s.%s" % (d["run_id"], d["step_id"]), body)


class Counter:
    """LogChunk.seq: monotonic per (run, step, attempt), so a viewer can see
    that it missed some."""

    def __init__(self):
        self.n = 0
        self.lock = threading.Lock()

    def next(self):
        with self.lock:
            self.n += 1
            return self.n


def decode_dispatch(data):
    f = parse(data)
    step = parse(one(f, 5, b"") or b"")
    inputs = []
    for raw in f.get(6, []):
        ref = parse(raw)
        digest = parse(one(ref, 2, b"") or b"")
        inputs.append(
            {
                "port": text(ref, 1),
                "key": text(ref, 3),
                "digest": (text(digest, 1), text(digest, 2)) if digest else None,
            }
        )
    secrets = []
    for raw in f.get(7, []):
        ref = parse(raw)
        secrets.append(
            {"name": text(ref, 1), "handle": text(ref, 2), "expires_at": one(ref, 3, 0)}
        )
    return {
        "run_id": text(f, 1),
        "step_id": text(f, 2),
        "attempt": one(f, 3, 0),
        "fence_token": text(f, 4),
        "inputs": inputs,
        "secrets": secrets,
        "output_prefix": text(f, 8),
        "protocol_version": one(f, 9, 0),
        "command": [c.decode() for c in f.get(11, [])],
        "env": decode_map(f, 12),
        # Copied forward wholesale rather than field by field, so an engine
        # that joins the run's trace keeps working when W3C grows a header.
        "trace_context": decode_map(f, 13),
        "outputs": [text(parse(p), 1) for p in step.get(6, [])],
        "capabilities": repeated_varint(step, 7),
    }


def platform_os():
    return {"linux": "linux", "darwin": "darwin", "win32": "windows"}.get(
        sys.platform, sys.platform
    )


def platform_arch():
    machine = os.uname().machine if hasattr(os, "uname") else ""
    return {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }.get(machine, machine)


def main():
    engine = Engine(ignore_cancel="--ignore-cancel" in sys.argv[1:])
    try:
        engine.run()
    except KeyboardInterrupt:
        pass
    finally:
        engine.running = False
        engine.nats.close()


if __name__ == "__main__":
    main()
