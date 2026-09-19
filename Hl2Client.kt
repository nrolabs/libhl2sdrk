/*
 * libhl2sdrk - Kotlin driver for the Hermes-Lite 2 SDR transceiver
 *
 * Network transport (UDP), streaming and control lifecycle. The wire format
 * itself lives in the Android-free [Hl2Protocol] codec, ported faithfully from
 * the reference driver (Quisk, by James Ahlstrom) and the openHPSDR Protocol 1
 * / Hermes-Lite 2 register documentation (Steve Haynal):
 *   https://github.com/softerhardware/Hermes-Lite2/blob/master/software/hl2setup/hl2.cxx
 *   https://github.com/softerhardware/Hermes-Lite2/wiki
 *
 * Kotlin port: Copyright (C) 2026 Isak Ruas <isakruas@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses/>.
 */

package com.isaklab.libhl2sdrk
import com.isaklab.isdrdrivers.core.LnaGainCapable
import com.isaklab.isdrdrivers.core.TxDriveCapable
import com.isaklab.isdrdrivers.core.TxTimingCapable
import com.isaklab.isdrdrivers.core.TransmitCapable
import com.isaklab.isdrdrivers.core.RadioClient

import android.util.Log
import com.isaklab.isdrdrivers.core.DspThread
import com.isaklab.isdrdrivers.core.FFTProcessor
import com.isaklab.isdrdrivers.core.SpectrumWorker
import com.isaklab.isdrdrivers.core.FloatRing
import com.isaklab.isdrdrivers.core.SeqTracker
import kotlinx.coroutines.*
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch

/**
 * Full RX + TX driver for a Hermes-Lite 2 over the network.
 *
 * RX: receives endpoint-6 frames, decodes 24-bit interleaved IQ into a
 * `FloatArray` (`i0,q0,…` in `[-1,1]`), accumulates display-sized blocks,
 * computes the power spectrum, and hands both to [onDataReceived] — the same
 * contract as the RTL-SDR clients so the app can drive either device.
 *
 * TX: a control frame is sent for every RX frame received (the board is
 * frame-locked), rotating the C0 register banks. While [setPtt] is on, queued
 * transmit IQ (fed via [submitTxIq], 48 kSps interleaved, `[-1,1]`) is streamed
 * to the transmit DAC; on underrun it sends silence.
 */
class Hl2Client(
    /** Board IP, or "255.255.255.255" (default) to discover on the LAN. */
    private val host: String = BROADCAST,
    /** (power spectrum in dB, interleaved IQ i0,q0,… in [-1,1]) */
    private val onDataReceived: (FloatArray, FloatArray) -> Unit,
    private val onConnectionStatusChanged: (Boolean, String) -> Unit,
    /**
     * Per-receiver IQ block (rx index, interleaved IQ), for receivers armed
     * via [setRxStreamMask]. Fired before [onDataReceived] of the same flush
     * over the same sample span, so blocks pair up sample-aligned.
     */
    private val onDataRx: ((Int, FloatArray) -> Unit)? = null,
    private val onTelemetry: ((Hl2Protocol.Telemetry) -> Unit)? = null,
    /** Board UDP port — override only if the firmware default (1024) was changed. */
    private val port: Int = Hl2Protocol.PORT,
    /** Exact product/wire profile; its expected board family is verified by discovery. */
    private val profile: Protocol1Profile = Protocol1Profile.HERMES_LITE_2,
    /** Optional read-only preflight performed by DriverSession before replacing an open radio. */
    private val verifiedBoard: VerifiedProtocol1Board? = null,
) : RadioClient, TransmitCapable, TxDriveCapable, TxTimingCapable, LnaGainCapable {
    companion object {
        const val BROADCAST = "255.255.255.255"
        private const val TAG = "Hl2Client"
        /** Per-receiver stream slots (protocol-1 hardware tops out at 4 DDCs). */
        private const val MAX_RECEIVERS = 4
        private val EMPTY_SPECTRUM = FloatArray(0)

        // Round-robin of C0 bank pairs. Beyond the classic Hermes banks, the
        // HL2 needs 0x16/0x17 (TX buffer latency + PTT hang — without it the
        // gateware default can drop the first TX syllable) and 0x3A (reset on
        // disconnect, so a dead client session frees the board).
        private val C0_SEQUENCE = intArrayOf(0, 2, 4, 6, 8, 10, 0x0E, 0x10, 0x16, 0x3A)

        // Classic Protocol-1 firmware only defines the documented banks —
        // the reference clients (hl2.cxx, PowerSDR) rotate 0..10 and nothing
        // else. Sending HL2-extension addresses to old firmware is undefined
        // behaviour, so the rotation excludes them.
        // Classic addr0x0E owns the typed RX->ADC router. Keep route first,
        // then the paired RX1/RX2 NCO bank, and advertise topology/sync last.
        // Enabling diversity resets the cursor under [sendLock] and emits
        // exactly this prefix before returning APPLIED to DriverSession.
        private val C0_SEQUENCE_CLASSIC = intArrayOf(0x0E, 2, 0, 4, 6, 8, 10)

        // Idle EP2 cadence: 3 ms while the control state is moving, 10 ms once
        // it has been still for half a second. A full C0 rotation still
        // completes in 100 ms at the slow rate, and nudge() leaves it at once.
        private const val IDLE_FAST_NS = 3_000_000L
        private const val IDLE_SLOW_NS = 10_000_000L
        private const val CONTROL_QUIET_NS = 500_000_000L
        // IO board: 12 RX fcode registers on the Pico; one delta batch per
        // 500 ms keeps I2C passthrough writes from crowding out C&C traffic
        // (the reference cadence of the board's own protocol notes).
        private const val IO_FCODE_SLOTS = 12
        private const val IO_PUSH_PERIOD_NS = 500_000_000L
        // Frames the teardown may spend flushing the queued neutral-routing
        // write once the control sender has stopped (one register per frame).
        private const val IO_NEUTRAL_FLUSH_FRAMES = 16
    }

    private val usesClassicCodec = profile.usesClassicCodec

    /**
     * When false, RX blocks skip the FFT and are delivered with an empty
     * spectrum array — the host has no visible spectrum consumer. Audio/IQ
     * delivery is unaffected.
     */
    @Volatile override var spectrumEnabled: Boolean = true

    /**
     * Narrow the panadapter's span before the transform; returns the
     * decimation actually in force. With no session there is nothing to hold
     * it, and the answer is an honest 1 rather than the request echoed back.
     */
    fun setSpectrumZoom(decimation: Int, offsetHz: Long): Int =
        sessionRef.get()?.spectrum?.setZoom(decimation, offsetHz.toDouble(), getSampleRate()) ?: 1


    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    /**
     * One connection generation: the socket, the two loop threads, the
     * spectrum path and the EP2 frame counter that belong to it.
     *
     * Everything a teardown has to touch lives in here, and every teardown
     * request carries the [Session] it was issued for. A late request
     * therefore dismantles ITS OWN objects and can never reach a session that
     * started in the meantime — there is no "is this generation still
     * current?" test to lose a race against, because the teardown never reads
     * a client field to find out what to close. The only shared things a
     * teardown still touches (the control state, the status callback, the
     * amp sequencer) are gated on the session still being the published one,
     * and each of those gates is a plain no-op when it fails.
     *
     * The two hot loops own their threads (see DspThread): they only block on
     * the socket and pace themselves, so a shared coroutine pool bought them
     * nothing and cost a leaked priority plus a thread hop per beat.
     */
    private class Session(
        val socket: DatagramSocket,
        val fft: FFTProcessor,
        // Welch FFT off the receive thread: an inline FFT stalls socket reads
        // and drops EP6 frames. The receive thread only submits on the display
        // cadence and delivers the last cached spectrum — audio never waits.
        val spectrum: SpectrumWorker,
    ) {
        /** Loop gate. Per session, so a new connect() cannot resurrect the
         *  loops of an old one. */
        @Volatile var running = false
        /** One-shot latch: the teardown of this session runs exactly once. */
        val teardownDone = java.util.concurrent.atomic.AtomicBoolean(false)
        /** Serialises the final connect publication against teardown. */
        val lifecycleLock = Any()
        /** A losing disconnect does not return before the winning teardown. */
        val teardownFinished = CountDownLatch(1)
        @Volatile var teardownOwner: Thread? = null
        /** Written by the connect coroutine, read by a teardown running on
         *  the receive thread. Volatile is load-bearing, not decoration: a
         *  teardown that reads a stale null [txSenderThread] skips the
         *  DspThread.stop below and the EP2 pacer keeps writing the socket
         *  while the unkey/stop frames go out — the exact sequence inversion
         *  the per-session teardown exists to remove. */
        @Volatile var receiveThread: Thread? = null
        @Volatile var txSenderThread: Thread? = null
        /** EP2 frame counter and C0 bank cursor. Per session (the board's own
         *  counting restarts with the session) and mutated inside the send
         *  critical section, so the frame numbered N is the frame sent N-th. */
        var txSeq = 0L
        var c0Index = 0

        /**
         * RX accumulation and frame continuity, per session and touched only
         * by this session's receive thread.
         *
         * These were client fields, and that was wrong twice over. Sequential
         * first: a session that ends mid-block leaves its partial block in the
         * accumulator, and the NEXT session delivers those stale samples glued
         * to the front of its first block — no race required, it happens on
         * every reconnect that does not land exactly on a block boundary. Then
         * concurrent: the teardown of the old session is asynchronous
         * (disconnect only posts it), so its receive thread can still be
         * decoding a frame while connect() builds the next session and starts
         * a second receive thread — two threads writing one FloatArray and one
         * pair counter, with no lock. Per session, each thread only ever
         * touches the object it was started for, and both problems are gone by
         * construction.
         */
        var accum = FloatArray(0)
        var accumPairs = 0
        val accumRx = arrayOfNulls<FloatArray>(MAX_RECEIVERS)
        val accumRxPairs = IntArray(MAX_RECEIVERS)
        /** EP6 frame-sequence continuity (bytes 4..7 of every RX frame). A gap
         *  is concealed with zeros of the estimated hole; a late reordered
         *  frame is dropped (out-of-order samples would corrupt the
         *  accumulator). The board restarts its numbering per session, so the
         *  tracker must too. */
        val ep6Seq = SeqTracker()
        /** Last-seen RX-frame status bits: the key/PTT inputs and ADC clip
         *  flag are EDGE information — telemetry must fire on every change
         *  even when the frame carries no AIN bank data (hasData false). */
        var lastKeyState = 0
        /** Display-cadence stamp for the off-thread FFT submissions. */
        var lastFftTimeMs = 0L
    }

    /** The published session, or null while disconnected. */
    private val sessionRef = java.util.concurrent.atomic.AtomicReference<Session?>(null)

    private val stateLock = Any()
    private val state = Hl2Protocol.ControlState().also {
        it.usesClassicCodec = usesClassicCodec
        it.classicPhysicalAdcCount = if (usesClassicCodec) profile.physicalAdcCount else 0
    }

    /** Spectrum smoothing chosen by the host; re-applied to each session's
     *  FFT so a reconnect does not silently fall back to the default. */
    @Volatile private var smoothingFactor = -1f

    /** Last time the control state moved, for the idle EP2 cadence. */
    @Volatile private var lastControlChangeNs = 0L

    // RX accumulation cadence. The buffers themselves live in the Session;
    // only the host-chosen block size is a client setting (it must survive a
    // reconnect).
    private val fftIntervalMs = 80L
    private var flushPairs = 800

    // TX sample queue: interleaved I/Q in [-1,1] at 48 kSps, drop-oldest.
    // Primitive ring — ArrayDeque<Float> boxed every sample (~100k boxes/s
    // while keyed), the same GC-storm class the RX sinks were rebuilt to kill.
    private val txQueue = FloatRing(Hl2Protocol.TX_RATE * 4)
    private val txLock = Any()

    /** RX discontinuity events (loss/reorder) on the live session — telemetry.
     *  Zero while disconnected: the counter belongs to the stream, and a
     *  session that is gone has no discontinuities left to report. */
    val rxGapCount: Long get() = sessionRef.get()?.ep6Seq?.gapEvents ?: 0L

    // Which hardware receiver feeds the spectrum/audio when 2 are configured.
    @Volatile private var activeReceiver = 0

    // Extra per-receiver streams (bit n = also deliver receiver n's IQ via
    // onDataRx). Both receivers are decoded from every frame regardless —
    // this only controls whether the non-active samples are kept or dropped.
    @Volatile private var rxStreamMask = 0

    /**
     * The IQ sink for one session: only the active receiver's samples reach
     * the display/audio accumulator, the rest go to their own only if armed.
     *
     * Built once per session (in the receive loop, before the first frame) and
     * reused for every frame after that — a sink allocated per frame is a
     * lambda per frame on the hot path, which is the GC churn the RX sinks
     * were rebuilt to remove. Binding it to [s] is what keeps the accumulator
     * writes inside the session that owns them.
     */
    private fun sinkFor(s: Session) = Hl2Protocol.SampleSink { rx, i, q ->
        if (rx == activeReceiver) appendSample(s, i, q)
        else if ((rxStreamMask shr rx) and 1 == 1) appendSampleRx(s, rx, i, q)
    }
    private val txPull: () -> Int? = {
        synchronized(txLock) {
            if (txQueue.size >= 2) {
                val i = Hl2Protocol.toS16(txQueue.read())
                val q = Hl2Protocol.toS16(txQueue.read())
                (i shl 16) or (q and 0xFFFF)
            } else null
        }
    }

    // ========================================================================
    // Connection lifecycle
    // ========================================================================

    /**
     * Initializes the UDP socket, optionally discovers the board if configured for broadcast,
     * and establishes the network streams. Starts the RX and TX background threads,
     * allocating a 2 MiB receive buffer to mitigate frame drops during OS scheduler stalls.
     *
     * @return true if successfully connected and threads are running, false on failure or timeout.
     */
    override suspend fun connect(): Boolean = withContext(Dispatchers.IO) {
        try {
            onConnectionStatusChanged(false, "Discovering ${profile.displayName}…")
            require(verifiedBoard == null || verifiedBoard.profile == profile) {
                "verified discovery token is for ${verifiedBoard?.profile}, requested $profile"
            }
            // Even an explicit IP must answer discovery with the expected
            // board-family id. Resolving an address is not proof that the
            // selected ANAN/HL2 is the device listening there.
            val verified = verifiedBoard ?: Protocol1Discovery.find(profile, host, port)
            if (verified == null) {
                onConnectionStatusChanged(
                    false,
                    "No ${profile.displayName} with board id ${profile.discoveryBoardId} found",
                )
                return@withContext false
            }
            val sock = DatagramSocket()
            sock.soTimeout = 1000
            sock.connect(verified.address, port)
            // 1032-byte frames every ~2.6 ms: the platform default socket
            // buffer rides through only ~100 ms of GC/scheduler stall. Ask
            // for 2 MiB (kernel may clamp).
            try { sock.receiveBufferSize = 2 * 1024 * 1024 } catch (_: Exception) {}
            val fft = FFTProcessor(800)
            if (smoothingFactor >= 0f) fft.setSmoothingFactor(smoothingFactor)
            val s = Session(sock, fft, SpectrumWorker(fft))
            // Publish before the handshake: if it throws, the catch below has
            // a session to tear the socket down with instead of leaking it.
            // Any teardown still owed by an earlier session now targets that
            // earlier object and cannot touch this one.
            sessionRef.set(s)
            updateFlushThreshold()
            // No sequence tracker to reset here any more: it is created with
            // the Session above. Resetting a shared one from this coroutine
            // reached into the receive thread of the session being torn down —
            // that thread reads it once per frame and would have seen the
            // counter jump backwards mid-stream.

            sendStartStop(s, false); delay(20)
            sendStartStop(s, false); delay(20)
            sendStartStop(s, true)
            // The IO board's registers survive between sessions: schedule the
            // reset-then-full-mirror so this connection never inherits stale
            // frequencies from a previous one.
            synchronized(ioLock) {
                ioResetDone = false
                if (ioEnabled) ioNeedsReset = true
            }
            val started = synchronized(s.lifecycleLock) {
                if (s.teardownDone.get() || sessionRef.get() !== s) {
                    false
                } else {
                    s.running = true
                    onConnectionStatusChanged(true, "Connected")
                    // A status callback may synchronously request disconnect.
                    // Recheck before creating either loop in that case.
                    if (s.teardownDone.get() || sessionRef.get() !== s) {
                        false
                    } else {
                        startReceiving(s)
                        startTxSender(s)
                        true
                    }
                }
            }
            if (!started) {
                cleanup(s, "Disconnected")
                return@withContext false
            }
            // A routing debt left by a previous session (the board was
            // disabled or the link dropped while J9 was selected) can only be
            // paid once there is a socket again. The write is queued here and
            // the control sender just started carries it out.
            if (ioNeutralOwed()) sendIoNeutral()
            true
        } catch (e: Exception) {
            Log.e(TAG, "connect failed: ${e.message}")
            onConnectionStatusChanged(false, "Connection failed")
            disconnect()
            false
        }
    }

    /**
     * Sends the hardware stop sequence to the board, cleanly halts background processing
     * threads, and shuts down the UDP socket. Thread-safe and safe to call when partially initialized.
     */
    override fun disconnect() {
        // Capture the session NOW. The coroutine below runs later — possibly
        // after a connect() has already brought a new session up — and it
        // must dismantle the one that was live when disconnect was asked for,
        // nothing else. Every step of the teardown, including the unkey and
        // the clearing of the keying state, is inside cleanup() and bound to
        // this object; nothing happens out here where a newer session could
        // be caught by it.
        val s = sessionRef.get()
        if (s == null) {
            // Never connected, or already torn down: the host still expects
            // the disconnected status.
            onConnectionStatusChanged(false, "Disconnected")
            return
        }
        // Close the control admission gate synchronously, then finish the
        // session-bound teardown before returning. cleanup() detects its own
        // loop thread and never self-joins.
        s.running = false
        cleanup(s, "Disconnected")
    }

    /**
     * Full teardown of [s], shared by [disconnect] and the receive loop's
     * timeout/error exits. Idempotent per session: whichever path gets here
     * first does the work, later callers return at once. Without this the
     * failure paths only cleared `running` — the UDP socket stayed open and
     * the spectrum worker thread stayed alive until the next connect.
     *
     * Safe to call from either loop thread (a thread never joins on itself)
     * and safe to call for a session that is no longer the published one: it
     * only ever closes [s]'s own socket and threads. The client [scope] is
     * left alive on purpose: it owns no threads (Dispatchers.IO) and
     * connect() may be called again.
     */
    private fun cleanup(s: Session, statusMessage: String) {
        if (!s.teardownDone.compareAndSet(false, true)) {
            awaitCleanupOwner(s)
            return
        }
        val self = Thread.currentThread()
        s.teardownOwner = self
        try {
            synchronized(s.lifecycleLock) {
                s.running = false
                // Stop the EP2 pacer BEFORE anything else is put on the wire.
                // It is the only other writer of this socket, and it builds
                // frames from the same txSeq/C0 cursor.
                s.txSenderThread?.takeIf { it !== self }?.let { DspThread.stop(it) }

                // Shared state belongs to whichever session is published. A
                // stale teardown only releases this session's own resources.
                if (sessionRef.get() === s) {
                    // Unkey ON THE WIRE, socket still open and pacer already stopped.
                    // setPtt only flips state.mox in memory and relies on the pacer's
                    // next frame; on any exit the pacer may never emit it, leaving
                    // the board holding the MOX=1 of the last frame it received. This
                    // is the one place that closes that window, for every exit path —
                    // operator disconnect mid-over as well as timeout/error — which
                    // is why the keying state is cleared HERE and not by the callers:
                    // clearing it earlier would make these frames redundant on the
                    // clean path and leave the real unkey to the failure path alone.
                    // Best-effort: on the failure paths the link may already be dead
                    // and the sends throw. The gateware's own TX watchdog remains the
                    // final protection — this only keeps it from ever being needed.
                    val wasKeyed = synchronized(stateLock) {
                        val keyed = state.mox || state.ampKeyed
                        state.mox = false; state.ampKeyed = false
                        keyed
                    }
                    if (wasKeyed) repeat(3) { runCatching { sendControlFrame(s) } }
                    // Hand the RX routing back to the internal input while the socket
                    // is still open. The board latches the J9 routing pins until a
                    // REG_RF_INPUTS = 0 write arrives, so a session that just ends
                    // with mode 1 or 2 selected leaves the main antenna dead.
                    if (ioNeutralOwed()) {
                        runCatching {
                            i2cWrite(
                                bus2 = true, device = IoBoard.ADDRESS,
                                reg = IoBoard.REG_RF_INPUTS, value = 0,
                            )
                            // The pacer is already stopped and a control frame carries
                            // at most one register write, so nothing else will flush
                            // the queue this write went into: pump frames here until
                            // it is empty. Bounded, so a queue that keeps refilling
                            // cannot hold the teardown open.
                            var frames = 0
                            while (frames++ < IO_NEUTRAL_FLUSH_FRAMES && ioWritePending()) sendControlFrame(s)
                            // The board never acknowledges a passthrough write, and an
                            // abrupt link loss (radio unplugged, Wi-Fi gone) takes the
                            // send with it. On that path the debt stays recorded and
                            // is paid at the next connect; until then the board keeps
                            // the routing it was left with. There is no way around it
                            // from the host side.
                            synchronized(ioLock) { ioNeutralPending = ioWritePending() }
                        }
                    }
                    runCatching { sendStartStop(s, false) }
                }
                // Close before joining RX: DatagramSocket.receive is not
                // interruptible without the close.
                s.socket.close()
                s.receiveThread?.takeIf { it !== self }?.let { DspThread.stop(it) }
                s.receiveThread = null
                s.txSenderThread = null
                s.spectrum.stop()
                if (sessionRef.compareAndSet(s, null)) {
                    onConnectionStatusChanged(false, statusMessage)
                }
            }
        } finally {
            s.teardownOwner = null
            s.teardownFinished.countDown()
        }
    }

    /** Wait for a concurrent owner, except on a loop it is currently joining. */
    private fun awaitCleanupOwner(s: Session) {
        val self = Thread.currentThread()
        if (
            s.teardownOwner === self ||
            s.receiveThread === self ||
            s.txSenderThread === self
        ) return
        var interrupted = false
        while (s.teardownFinished.count != 0L) {
            try {
                s.teardownFinished.await()
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (interrupted) self.interrupt()
    }

    // ========================================================================
    // RX
    // ========================================================================

    private fun startReceiving(s: Session) {
        // Audio priority on a thread of our own: it feeds the audio pipeline,
        // and on a low-end phone the spectrum/waterfall rendering otherwise
        // preempted it, delaying block delivery and popping the audio.
        // Created, published, then started: a teardown that lands in this
        // window must be able to see the handle it is expected to stop.
        val t = DspThread.create("hl2-rx", DspThread.PRIORITY_RADIO, onFailure = { failSession(s) }) {
            s.spectrum.start()
            val sink = sinkFor(s)
            val buf = ByteArray(2048)
            val packet = DatagramPacket(buf, buf.size)
            var noData = 0
            // The gate is this session's own flag: a later connect() raising
            // its own flag can never keep this thread iterating.
            while (s.running) {
                try {
                    s.socket.receive(packet)
                    noData = 0
                    if (Hl2Protocol.isRxFrame(buf, packet.length)) {
                        val nRx = synchronized(stateLock) { state.receiverCount }
                        // EP6 sequence (bytes 4..7) BEFORE decoding: conceal
                        // holes with zeros, drop late reordered frames.
                        val seq = ((buf[4].toLong() and 0xFF) shl 24) or
                            ((buf[5].toLong() and 0xFF) shl 16) or
                            ((buf[6].toLong() and 0xFF) shl 8) or
                            (buf[7].toLong() and 0xFF)
                        val missing = s.ep6Seq.advance(seq)
                        if (missing == -1L) continue
                        if (missing > 0) {
                            concealGap(s, missing * 2 * Hl2Protocol.samplesPerSubframe(nRx), nRx)
                        }
                        var telem = Hl2Protocol.parseRxFrame(buf, nRx, sink)
                        // Classic boards put exciter power / another AIN in
                        // the HL2's temperature/current slots — never claim
                        // those channels for them (fwd/rev/volts stay valid).
                        if (usesClassicCodec) telem = telem.copy(
                            hasTemperature = false, hasCurrent = false,
                        )
                        if (s.accumPairs >= flushPairs) flushRx(s)
                        val keyState = (if (telem.keyPtt) 1 else 0) or
                            (if (telem.keyDash) 2 else 0) or
                            (if (telem.keyDot) 4 else 0) or
                            (if (telem.adcOverflow) 8 else 0)
                        if (telem.hasData || keyState != s.lastKeyState) {
                            s.lastKeyState = keyState
                            onTelemetry?.invoke(telem)
                        }
                    }
                } catch (e: SocketTimeoutException) {
                    if (++noData > 5) {
                        try { sendStartStop(s, true) } catch (_: Exception) {}
                        if (noData > 15) {
                            // Full teardown, not just running=false: leaving
                            // the socket open and the spectrum worker running
                            // held resources until the next connect.
                            cleanup(s, "Timeout")
                        }
                    }
                } catch (e: Exception) {
                    if (s.running) {
                        Log.e(TAG, "rx error: ${e.message}")
                        cleanup(s, "Error")
                    }
                }
            }
        }
        s.receiveThread = t
        t.start()
    }

    /**
     * Retire [s] after one of its loop threads died on an unhandled throwable.
     * The socket is the usual reason (a concurrent teardown closes it while a
     * send is in flight), and it is not an error the operator can act on, so
     * the status text only says the session ended; the throwable itself is
     * already logged by DspThread. If the teardown is what killed the thread,
     * cleanup's one-shot latch makes this a no-op.
     */
    private fun failSession(s: Session) {
        cleanup(s, if (s.running) "Error" else "Disconnected")
    }

    /**
     * Sends control/transmit frames, decoupled from RX reception, so the
     * downstream stays at 48 kSps. While transmitting, a frame (126 samples) is
     * sent only once a full frame of microphone IQ is queued — this locks the
     * transmit rate to the mic clock. Tying transmit to the RX frame rate
     * over-feeds it (a 2-receiver stream arrives at ~666 frames/s while the mic
     * makes 48 k/s), which starved the queue and made the audio choppy. When
     * idle it sends steady control frames so register writes and the board
     * keepalive keep flowing.
     */
    private fun startTxSender(s: Session) {
        val t = DspThread.create("hl2-tx", DspThread.PRIORITY_RADIO, onFailure = { failSession(s) }) {
            val pairsPerFrame = 2 * Hl2Protocol.SAMPLES_PER_SUBFRAME      // samples per frame
            var txStartNs = 0L
            var framesSent = 0L
            while (s.running) {
                val transmitting = synchronized(stateLock) { state.mox }
                // Both branches: a retune while keyed (split, RIT) must reach
                // the IO board too, or an auto-tracking amp keys the old band.
                maybePushIoBoard()
                if (transmitting) {
                    // Pace frames against the wall clock and send in bursts:
                    // one frame per delay(1) wake-up cannot reach the ~381 fps
                    // a 126-sample frame period (2.6 ms) needs — Android's
                    // delay granularity is 1–10 ms and TX starved at ~42.7 kS/s
                    // (choppy transmit audio in the field).
                    val now = System.nanoTime()
                    if (txStartNs == 0L) { txStartNs = now; framesSent = 0L }
                    val targetFrames = (now - txStartNs) * Hl2Protocol.TX_RATE.toLong() /
                        (pairsPerFrame * 1_000_000_000L)
                    if (targetFrames - framesSent > 32) {
                        // Mic underrun starved the queue: rebase instead of
                        // dumping a stale catch-up burst at the radio.
                        txStartNs = now
                        framesSent = 0L
                    } else {
                        var burst = 0
                        // s.running is re-read every frame, not just per outer
                        // iteration: a burst is up to 16 syscalls long and a
                        // teardown only waits 500 ms for this thread to join.
                        // If the join expires (a GC pause or a loaded CPU is
                        // enough) the socket is closed under the burst, and a
                        // burst that could not notice would send into it.
                        while (s.running && framesSent < targetFrames && burst < 16) {
                            // Send the paced frame even when the queue is
                            // starved: buildControlFrame fills missing samples
                            // with silence. The old queue-ready gate stopped
                            // ALL EP2 traffic on underrun — register writes
                            // (including a pending unkey in the C0 rotation)
                            // and the board keepalive stalled, and the DUC
                            // underran with no soft fill (audit A5).
                            sendControlFrame(s)
                            framesSent++
                            burst++
                        }
                    }
                    // 0.5 ms beat: fine enough to hit the ~381 fps a 126-sample
                    // frame period needs, and it no longer goes through the
                    // coroutine timer thread.
                    DspThread.pace(System.nanoTime() + 500_000L)
                } else {
                    txStartNs = 0L
                    sendControlFrame(s)
                    // IDLE CADENCE. The board only needs EP2 traffic flowing for
                    // its keepalive and for the C0 bank rotation to carry
                    // register writes; 333 frames/s of that while merely
                    // LISTENING is a syscall every 3 ms for nothing. Stay fast
                    // while the control state is actually moving (the operator
                    // is turning the dial, a one-shot write is queued, the key
                    // is down) and fall back to 100/s once nothing has changed
                    // for half a second. Any setter that the operator can feel
                    // calls nudge(), so leaving the slow cadence is immediate —
                    // it is never the reason a control change waits.
                    val quietNs = System.nanoTime() - lastControlChangeNs
                    val period = if (quietNs > CONTROL_QUIET_NS) IDLE_SLOW_NS else IDLE_FAST_NS
                    DspThread.pace(System.nanoTime() + period)
                }
            }
        }
        s.txSenderThread = t
        t.start()
    }

    /**
     * Wake the control sender now and hold the fast cadence for a moment: an
     * operator-visible change (dial, rate, gain, key, register write) must not
     * wait out the idle period.
     */
    private fun nudge() {
        lastControlChangeNs = System.nanoTime()
        java.util.concurrent.locks.LockSupport.unpark(sessionRef.get()?.txSenderThread)
    }

    /** A retained client object is not an active terminal control session. */
    private fun requireControlSession(): Session {
        val s = sessionRef.get()
            ?: throw IllegalStateException("Protocol-1 control sender is not running")
        check(s.running && !s.socket.isClosed) {
            "Protocol-1 control sender is not running"
        }
        return s
    }

    /** A failed UDP transaction leaves hardware state unknowable; retire it visibly. */
    private fun retireAfterControlFailure(s: Session, operation: String) {
        scope.launch { cleanup(s, "$operation failed; Protocol-1 session retired") }
    }

    /**
     * Insert [pairsPerRx] zero IQ pairs (capped at one display block) into
     * the active accumulator — and every armed per-receiver stream, so the
     * EV_DATA_RX / EV_DATA sample alignment survives the hole — then restart
     * the spectrum smoothing so the IIR doesn't blend across the gap.
     */
    private fun concealGap(s: Session, pairsPerRx: Long, nRx: Int) {
        val n = pairsPerRx.coerceAtMost(flushPairs.toLong()).toInt()
        repeat(n) {
            appendSample(s, 0f, 0f)
            for (rx in 0 until nRx) {
                if (rx != activeReceiver && (rxStreamMask shr rx) and 1 == 1) {
                    appendSampleRx(s, rx, 0f, 0f)
                }
            }
        }
        s.spectrum.resetSmoothing()
        Log.w(TAG, "ep6 gap: $pairsPerRx pairs lost (events=${s.ep6Seq.gapEvents})")
    }

    private fun appendSample(s: Session, i: Float, q: Float) {
        val need = (s.accumPairs + 1) * 2
        if (s.accum.size < need) s.accum = s.accum.copyOf(maxOf(need, flushPairs * 2))
        s.accum[s.accumPairs * 2] = i
        s.accum[s.accumPairs * 2 + 1] = q
        s.accumPairs++
    }

    private fun appendSampleRx(s: Session, rx: Int, i: Float, q: Float) {
        if (rx !in 0 until MAX_RECEIVERS) return
        var a = s.accumRx[rx]
        val need = (s.accumRxPairs[rx] + 1) * 2
        if (a == null || a.size < need) {
            a = (a ?: FloatArray(0)).copyOf(maxOf(need, flushPairs * 2))
            s.accumRx[rx] = a
        }
        a[s.accumRxPairs[rx] * 2] = i
        a[s.accumRxPairs[rx] * 2 + 1] = q
        s.accumRxPairs[rx]++
    }

    private fun flushRx(s: Session) {
        // Armed per-receiver streams first: same flush, same sample span, so
        // the consumer pairs each EV_DATA_RX with the following EV_DATA.
        //
        // Every receiver is visited on every flush, armed or not. Skipping the
        // whole loop while the mask is clear (or skipping a receiver whose bit
        // has just been cleared) leaves that receiver's accumulator holding
        // the samples it had when it was disarmed — and those stale samples
        // then come out GLUED to the front of the first block after it is
        // re-armed, shifting the EV_DATA_RX/EV_DATA alignment above by the
        // size of the leftover for the rest of the session. Disarmed
        // receivers are drained here, on the receive thread, which is also
        // what makes this thread the only writer of [accumRxPairs].
        val mask = rxStreamMask
        val blockPairs = s.accumPairs
        for (rx in 0 until MAX_RECEIVERS) {
            val pairs = s.accumRxPairs[rx]
            if (pairs == 0) continue
            s.accumRxPairs[rx] = 0
            if ((mask shr rx) and 1 == 0) continue
            val cb = onDataRx ?: continue
            // Length-matched to the active block, always. A receiver armed
            // part-way through a block has fewer pairs than the block spans,
            // and they are its LAST ones — so they go at the END and the head
            // is zero-filled. Handing over the short array instead would make
            // the consumer read those samples as the START of the span and
            // pair them with the wrong part of the active block, which for
            // diversity or PureSignal is a phase error, not a missing sample.
            val out = FloatArray(blockPairs * 2)
            val take = minOf(pairs, blockPairs)
            System.arraycopy(
                s.accumRx[rx]!!, (pairs - take) * 2,
                out, (blockPairs - take) * 2, take * 2,
            )
            cb.invoke(rx, out)
        }
        val block = s.accum.copyOf(blockPairs * 2)
        s.accumPairs = 0
        // With every spectrum consumer hidden the FFT is pure waste on a phone
        // CPU — deliver the IQ with an empty spectrum instead (audio must never
        // depend on the FFT path).
        if (!spectrumEnabled) {
            onDataReceived(EMPTY_SPECTRUM, block)
            return
        }
        // The display renders ~10 fps: on that cadence hand a copy of the block
        // to the off-thread worker (submit copies internally); the FFT never
        // runs on this receive thread. Deliver the IQ NOW with the last cached
        // spectrum — audio must never wait on the FFT (spectrum may lag one
        // frame, imperceptible at 10 fps).
        val now = System.currentTimeMillis()
        if (now - s.lastFftTimeMs >= fftIntervalMs) {
            s.lastFftTimeMs = now
            s.spectrum.submit(block, block.size / 2)
        }
        onDataReceived(s.spectrum.latest ?: EMPTY_SPECTRUM, block)
    }

    // ========================================================================
    // TX / control
    // ========================================================================

    /**
     * Serialises everything this client puts on the socket, and nothing else.
     *
     * `DatagramSocket.send` is NOT a guaranteed-immediate syscall: java.net
     * datagram sockets are blocking, and a full transmit buffer (ENOBUFS on a
     * congested or fading Wi-Fi link, or a driver queue that has stopped) can
     * park the caller inside `sendto`. Whatever lock is held across it is held
     * for that whole stall — which is why it must not be [stateLock]. That one
     * is taken by the UI on every dial/rate/PTT touch AND by the receive
     * thread once per RX frame (333–3000 times a second at the rates the board
     * streams), so a single stalled transmit would take the audio path down
     * with it.
     *
     * This lock is only ever taken by the two writers of the socket — the EP2
     * pacer and the teardown's unkey/stop frames — so a stall blocks the other
     * sender and no one else. The ordering guarantee the EP2 counter needs is
     * unchanged and is exactly why the lock exists: the frame numbered N is
     * still the frame that leaves first, because numbering and sending happen
     * inside the same critical section. [stateLock] is now taken only for the
     * frame BUILD, an in-memory copy with no syscall in it.
     *
     * Lock order is sendLock → stateLock → txLock, everywhere, and nothing
     * takes sendLock while holding either of the other two.
     */
    private val sendLock = Any()

    private fun sendStartStop(s: Session, start: Boolean) {
        val b = Hl2Protocol.startStop(start)
        synchronized(sendLock) { s.socket.send(DatagramPacket(b, b.size)) }
    }

    private fun sendControlFrame(s: Session) {
        val sequence = if (usesClassicCodec) C0_SEQUENCE_CLASSIC else C0_SEQUENCE
        synchronized(sendLock) {
            val frame = synchronized(stateLock) {
                if (state.oneShotAddr < 0 && oneShotQueue.isNotEmpty()) {
                    val (a, d) = oneShotQueue.removeFirst()
                    state.oneShotAddr = a; state.oneShotData = d
                }
                val f = Hl2Protocol.buildControlFrame(
                    s.txSeq++, sequence[s.c0Index % sequence.size], state, txPull,
                )
                s.c0Index = (s.c0Index + 1) % sequence.size
                f
            }
            s.socket.send(DatagramPacket(frame, frame.size))
        }
    }

    /**
     * Emit one exact C0 bank while [sendLock] is held by a semantic topology
     * transaction. Unlike the ordinary round-robin sender this deliberately
     * does not consume an unrelated one-shot register queued by the IO board.
     */
    private fun sendExactControlBankLocked(s: Session, c0: Int) {
        val frame = synchronized(stateLock) {
            check(state.oneShotAddr < 0) {
                "a one-shot register was already active during a receiver route transaction"
            }
            Hl2Protocol.buildControlFrame(s.txSeq++, c0, state, txPull)
        }
        s.socket.send(DatagramPacket(frame, frame.size))
    }

    /**
     * Apply one semantic state change and synchronously put its exact C0 bank
     * on the wire before returning. A failed write restores the in-memory
     * value and retires the now-indeterminate UDP session; callers therefore
     * cannot turn a local mutation into a false terminal success.
     */
    private fun <T> applyExactControlState(
        s: Session,
        c0: Int,
        operation: String,
        mutate: (Hl2Protocol.ControlState) -> T,
        rollback: (Hl2Protocol.ControlState, T) -> Unit,
    ) {
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) { mutate(state) }
            try {
                sendExactControlBankLocked(s, c0)
            } catch (failure: Exception) {
                synchronized(stateLock) { rollback(state, previous) }
                retireAfterControlFailure(s, operation)
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    private fun <T> applyExactControlState(
        c0: Int,
        operation: String,
        mutate: (Hl2Protocol.ControlState) -> T,
        rollback: (Hl2Protocol.ControlState, T) -> Unit,
    ) {
        applyExactControlState(requireControlSession(), c0, operation, mutate, rollback)
    }

    // ========================================================================
    // Public control API (mirrors the RTL clients, plus TX)
    // ========================================================================

    /**
     * Retunes the primary receiver's local oscillator through the same exact,
     * terminal bank write used by every indexed DDC tune.
     */
    override fun setFrequency(hz: Long) {
        setRxFrequency(0, hz)
    }

    /** Set any receiver's NCO frequency (0..3), openHPSDR addr 0x02..0x05. */
    fun setRxFrequency(index: Int, hz: Long) {
        val count = synchronized(stateLock) { state.receiverCount }
        require(index in 0 until count) {
            "receiver $index is outside configured count $count"
        }
        require(hz in 10_000L..(if (usesClassicCodec) 61_400_000L else 38_400_000L)) {
            "RX frequency $hz Hz is outside this board's exact tuning range"
        }
        val s = requireControlSession()
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) { state.rxFreqHz.copyOf() }
            synchronized(stateLock) {
                if (state.diversityMode == DiversityMode.RX1_RX2 && index < 2) {
                    state.rxFreqHz[0] = hz
                    state.rxFreqHz[1] = hz
                } else {
                    state.rxFreqHz[index] = hz
                }
            }
            try {
                sendExactControlBankLocked(s, if (index < 2) 2 else 4)
            } catch (failure: Exception) {
                synchronized(stateLock) { previous.copyInto(state.rxFreqHz) }
                retireAfterControlFailure(s, "receiver-frequency transaction")
                throw failure
            }
        }
        if (index == 0) s.spectrum.resetSmoothing()
        lastControlChangeNs = System.nanoTime()
    }

    /**
     * Sets the baseband sample rate for all configured receivers. 
     * Resets internal flushing boundaries and FFT smoothing to accommodate the new frame geometry.
     */
    override fun setSampleRate(hz: Int) {
        val ladder = Hl2Protocol.RATE_TO_SPEED.keys
        require(hz in ladder) { "sample rate $hz is not one of ${ladder.sorted()}" }
        val s = requireControlSession()
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) { state.sampleRate.also { state.sampleRate = hz } }
            try {
                sendExactControlBankLocked(s, 0)
            } catch (failure: Exception) {
                synchronized(stateLock) { state.sampleRate = previous }
                retireAfterControlFailure(s, "sample-rate transaction")
                throw failure
            }
        }
        updateFlushThreshold()
        s.spectrum.resetSmoothing()
        lastControlChangeNs = System.nanoTime()
    }

    /**
     * Sets the LNA gain. Applies internally to the step attenuator encoding 
     * depending on whether the board is identified as a classic protocol-1 model or a true HL2.
     * Range: -12 to +48 dB.
     */
    override fun setLnaGain(db: Int) {
        require(db in -12..48) { "LNA gain $db dB is outside -12..48" }
        applyExactControlState(
            c0 = 10,
            operation = "LNA-gain transaction",
            mutate = { it.lnaGainDb.also { _ -> it.lnaGainDb = db } },
            rollback = { state, previous -> state.lnaGainDb = previous },
        )
    }

    /** Configure the exact requested hardware-receiver count. */
    // HL2 gateware supports up to 4 DDCs; the frame slot math (6*nRx+2,
    // 504 usable bytes) holds for all four. No artificial 2-RX cap.
    fun setReceiverCount(n: Int) {
        require(n in 1..profile.receiverCapacity) {
            "receiver count $n is outside ${profile.displayName} capacity 1..${profile.receiverCapacity}"
        }
        require(activeReceiver < n) { "active receiver $activeReceiver is outside count $n" }
        require(rxStreamMask and ((1 shl n) - 1).inv() == 0) {
            "receiver stream mask references a removed receiver"
        }
        require(
            synchronized(stateLock) {
                state.diversityMode != DiversityMode.RX1_RX2 || n >= 2
            },
        ) { "RX1/RX2 diversity cannot survive receiver count $n" }
        val s = requireControlSession()
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) {
                state.receiverCount.also { state.receiverCount = n }
            }
            try {
                sendExactControlBankLocked(s, 0)
            } catch (failure: Exception) {
                synchronized(stateLock) { state.receiverCount = previous }
                retireAfterControlFailure(s, "receiver-count transaction")
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    /** Set receiver 2's frequency (kept for the existing command; generalized by setRxFrequency). */
    fun setFrequency2(hz: Long) {
        setRxFrequency(1, hz)
    }

    /** Choose which configured receiver drives the spectrum/waterfall and audio. */
    fun setActiveReceiver(index: Int) {
        val count = synchronized(stateLock) { state.receiverCount }
        require(index in 0 until count) { "receiver $index is outside configured count $count" }
        require(rxStreamMask and (1 shl index) == 0) {
            "active receiver $index is already an additional stream"
        }
        require(
            synchronized(stateLock) {
                state.diversityMode != DiversityMode.RX1_RX2 || index == 0
            },
        ) { "classic RX1/RX2 diversity requires receiver 0 as its reference" }
        val s = requireControlSession()
        activeReceiver = index
        s.spectrum.resetSmoothing()
    }

    /** True only for a discovered exact classic profile with two proven ADCs. */
    fun supportsDiversity(): Boolean = profile.diversitySupported

    /** Board-specific TX-feedback routing proved by this codec. */
    fun supportsPureSignal(): Boolean = profile.pureSignalSupported

    fun diversityMode(): DiversityMode = synchronized(stateLock) { state.diversityMode }

    /** Current typed physical route for one configured classic DDC. */
    fun rxAdc(receiver: Int): RxAdc {
        require(usesClassicCodec) { "${profile.displayName} has no classic RX-to-ADC router" }
        val count = synchronized(stateLock) { state.receiverCount }
        require(receiver in 0 until profile.receiverCapacity) {
            "receiver $receiver is outside ${profile.displayName} capacity ${profile.receiverCapacity}"
        }
        require(receiver < count) { "receiver $receiver is not configured (count=$count)" }
        return synchronized(stateLock) { state.rxAdc[receiver] }
    }

    /**
     * Route one configured classic DDC to a proven physical ADC. A rejected
     * request leaves both memory and wire state untouched.
     */
    fun setRxAdc(receiver: Int, adc: RxAdc) {
        require(usesClassicCodec) { "${profile.displayName} has no classic RX-to-ADC router" }
        require(adc.protocol1Code < profile.physicalAdcCount) {
            "$adc is unavailable on ${profile.displayName} (${profile.physicalAdcCount} ADC)"
        }
        require(receiver in 0 until profile.receiverCapacity) {
            "receiver $receiver is outside ${profile.displayName} capacity ${profile.receiverCapacity}"
        }
        val s = sessionRef.get()
            ?: throw IllegalStateException("Protocol-1 control sender is not running")
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) {
                require(receiver < state.receiverCount) {
                    "receiver $receiver is not configured (count=${state.receiverCount})"
                }
                if (state.diversityMode == DiversityMode.RX1_RX2 && receiver < 2) {
                    val other = 1 - receiver
                    require(state.rxAdc[other] != adc) {
                        "RX1/RX2 diversity requires distinct physical ADC inputs"
                    }
                }
                state.rxAdc[receiver].also { state.rxAdc[receiver] = adc }
            }
            if (previous == adc) return
            try {
                sendExactControlBankLocked(s, 0x0E)
                s.c0Index = 1 // paired RX1/RX2 NCO bank follows the route
            } catch (failure: Exception) {
                synchronized(stateLock) { state.rxAdc[receiver] = previous }
                retireAfterControlFailure(s, "RX-to-ADC route transaction")
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    /**
     * Apply the fixed classic phase-coherent pair as one wire transaction.
     *
     * Enable order is addr0x0E route -> RX1/RX2 NCO bank -> C0=0 sync bit.
     * Disable sends C0=0 with sync clear before a caller may edit routes.
     */
    fun setDiversityMode(mode: DiversityMode) {
        require(usesClassicCodec) { "${profile.displayName} has no classic diversity router" }
        require(profile.diversitySupported || mode == DiversityMode.DISABLED) {
            "${profile.displayName} has only ${profile.physicalAdcCount} physical ADC"
        }
        val s = sessionRef.get()
            ?: throw IllegalStateException("Protocol-1 control sender is not running")
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previousMode: DiversityMode
            val previousRoutes: Array<RxAdc>
            val previousRx2Hz: Long
            synchronized(stateLock) {
                if (state.diversityMode == mode) return
                if (mode == DiversityMode.RX1_RX2) {
                    require(state.receiverCount >= 2) {
                        "RX1/RX2 diversity needs two configured receivers"
                    }
                    require(activeReceiver == 0) {
                        "RX1/RX2 diversity requires receiver 0 as reference"
                    }
                }
                previousMode = state.diversityMode
                previousRoutes = state.rxAdc.copyOf()
                previousRx2Hz = state.rxFreqHz[1]
                state.diversityMode = mode
                if (mode == DiversityMode.RX1_RX2) {
                    state.rxAdc[0] = RxAdc.ADC1
                    state.rxAdc[1] = RxAdc.ADC2
                    state.rxFreqHz[1] = state.rxFreqHz[0]
                }
            }
            try {
                if (mode == DiversityMode.RX1_RX2) {
                    sendExactControlBankLocked(s, 0x0E)
                    sendExactControlBankLocked(s, 2)
                    sendExactControlBankLocked(s, 0)
                    s.c0Index = 3
                } else {
                    sendExactControlBankLocked(s, 0)
                    s.c0Index = 0
                }
            } catch (failure: Exception) {
                synchronized(stateLock) {
                    state.diversityMode = previousMode
                    previousRoutes.copyInto(state.rxAdc)
                    state.rxFreqHz[1] = previousRx2Hz
                }
                retireAfterControlFailure(s, "diversity transaction")
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    /** Exact semantic adapter for the app/driver wire command. */
    fun setDiversity(enabled: Boolean, referenceReceiver: Int, memberMask: Int) {
        if (enabled) {
            require(referenceReceiver == 0 && memberMask == 0b10) {
                "classic Protocol-1 supports only reference=0/memberMask=0b10"
            }
            setDiversityMode(DiversityMode.RX1_RX2)
        } else {
            require(memberMask == 0) { "disabled diversity must use memberMask=0" }
            require(referenceReceiver == activeReceiver) {
                "disabled diversity reference $referenceReceiver is not active $activeReceiver"
            }
            setDiversityMode(DiversityMode.DISABLED)
        }
    }

    /** Bit n = also stream receiver n's IQ via the onDataRx callback. */
    // The accumulators are NOT cleared here. This runs on the caller's thread
    // while the receive thread may be inside appendSampleRx, and a cross-
    // thread reset there is a torn count (samples written past a counter that
    // is about to be zeroed). The next flush drains every disarmed receiver
    // anyway, on the thread that owns those arrays.
    fun setRxStreamMask(mask: Int) {
        val count = synchronized(stateLock) { state.receiverCount }
        val allowed = (1 shl count) - 1
        require(mask >= 0 && mask and allowed.inv() == 0) {
            "receiver stream mask 0x${mask.toString(16)} exceeds 0x${allowed.toString(16)}"
        }
        require(mask and (1 shl activeReceiver) == 0) {
            "receiver stream mask contains active/reference receiver $activeReceiver"
        }
        requireControlSession()
        rxStreamMask = mask
    }

    fun getActiveReceiver(): Int = activeReceiver

    fun getSampleRate(): Double = synchronized(stateLock) { state.sampleRate.toDouble() }

    override fun frequencyHz(): Long = synchronized(stateLock) { state.rxFreqHz[0] }

    override fun sampleRateHz(): Int = synchronized(stateLock) { state.sampleRate }

    fun setSmoothingFactor(alpha: Float) {
        smoothingFactor = alpha                       // survives a reconnect
        sessionRef.get()?.fft?.setSmoothingFactor(alpha)
    }

    override fun setTxFrequency(hz: Long): Boolean {
        require(hz in 10_000L..(if (usesClassicCodec) 61_400_000L else 38_400_000L)) {
            "TX frequency $hz Hz is outside this board's exact tuning range"
        }
        applyExactControlState(
            c0 = 0,
            operation = "transmit-frequency transaction",
            mutate = { it.txFreqHz.also { _ -> it.txFreqHz = hz } },
            rollback = { state, previous -> state.txFreqHz = previous },
        )
        return true
    }

    // TX sequencing for a linear amp: on key-DOWN raise the amp/relay OC
    // line, wait txDelayMs (relay settle), then enable RF; on key-UP drop RF
    // first, wait hangMs (hot-switch protection), then release the relay.
    // With no amp configured (mask 0, delays 0) all three flags move together
    // and it behaves exactly as before.
    @Volatile private var ampTxDelayMs = 0
    @Volatile private var ampHangMs = 0
    fun setAmpKey(mask: Int, txDelayMs: Int, hangMs: Int) {
        require(mask >= 0 && mask and 0x7F.inv() == 0) {
            "amplifier key mask 0x${mask.toString(16)} exceeds 0x7f"
        }
        require(txDelayMs in 0..1000) { "amplifier TX delay $txDelayMs is outside 0..1000 ms" }
        require(hangMs in 0..1000) { "amplifier hang $hangMs is outside 0..1000 ms" }
        applyExactControlState(
            c0 = 0,
            operation = "amplifier-key policy transaction",
            mutate = {
                Triple(it.ampKeyMask, ampTxDelayMs, ampHangMs).also { _ ->
                    it.ampKeyMask = mask
                    ampTxDelayMs = txDelayMs
                    ampHangMs = hangMs
                }
            },
            rollback = { state, previous ->
                state.ampKeyMask = previous.first
                ampTxDelayMs = previous.second
                ampHangMs = previous.third
            },
        )
    }

    /**
     * TX buffer timing — addr 0x17 (C4[6:0] latency ms, C3[4:0] hang). The
     * register is part of the regular C0 rotation, so a change here reaches
     * the board on the next 0x16/0x17 bank pass; nudge() forces the fast
     * cadence so that happens within a rotation, not after the idle timer.
     * Classic Protocol-1 firmware does not define addr 0x17 (its rotation
     * excludes the HL2-extension banks), so on a classic board the values are
     * stored but never sent.
     */
    override fun setTxTiming(latencyMs: Int, hangMs: Int) {
        require(latencyMs in 0..127) { "TX latency $latencyMs is outside 0..127 ms" }
        require(hangMs in 0..31) { "PTT hang $hangMs is outside 0..31 ms" }
        require(!usesClassicCodec) {
            "${profile.displayName} has no Protocol-1 TX-buffer timing register"
        }
        applyExactControlState(
            c0 = 0x16,
            operation = "TX-buffer timing transaction",
            mutate = {
                Pair(it.txLatencyMs, it.pttHang).also { _ ->
                    it.txLatencyMs = latencyMs
                    it.pttHang = hangMs
                }
            },
            rollback = { state, previous ->
                state.txLatencyMs = previous.first
                state.pttHang = previous.second
            },
        )
    }

    override fun setPtt(on: Boolean) {
        val s = requireControlSession()
        val sequenced = (ampTxDelayMs > 0 || ampHangMs > 0) &&
            synchronized(stateLock) { state.ampKeyMask != 0 }
        if (!sequenced) {
            applyExactControlState(
                s = s,
                c0 = 0,
                operation = "PTT transaction",
                mutate = {
                    Pair(it.mox, it.ampKeyed).also { _ ->
                        it.mox = on
                        it.ampKeyed = on
                    }
                },
                rollback = { state, previous ->
                    state.mox = previous.first
                    state.ampKeyed = previous.second
                },
            )
            if (!on) {
                synchronized(txLock) { txQueue.clear() }
                // Unkey: spectrum jumps from TX leakage back to band noise —
                // restart the smoothing IIR instead of cross-fading the ghost.
                sessionRef.get()?.spectrum?.resetSmoothing()
            }
            return
        }
        if (on) {
            // Amp/relay OC line up first; RF (mox) after the settle delay.
            applyExactControlState(
                s = s,
                c0 = 0,
                operation = "amplifier-key transaction",
                mutate = {
                    Pair(it.mox, it.ampKeyed).also { _ ->
                        it.ampKeyed = true
                        it.mox = false
                    }
                },
                rollback = { state, previous ->
                    state.mox = previous.first
                    state.ampKeyed = previous.second
                },
            )
            waitForSequencerDelay(s, ampTxDelayMs, "amplifier key-up")
            applyExactControlState(
                s = s,
                c0 = 0,
                operation = "PTT key-up transaction",
                mutate = { it.mox.also { _ -> it.mox = true } },
                rollback = { state, previous -> state.mox = previous },
            )
        } else {
            // RF off immediately; hold the amp/relay for the hang, then drop.
            applyExactControlState(
                s = s,
                c0 = 0,
                operation = "PTT key-down transaction",
                mutate = { it.mox.also { _ -> it.mox = false } },
                rollback = { state, previous -> state.mox = previous },
            )
            synchronized(txLock) { txQueue.clear() }
            s.spectrum.resetSmoothing()
            waitForSequencerDelay(s, ampHangMs, "amplifier hang")
            applyExactControlState(
                s = s,
                c0 = 0,
                operation = "amplifier release transaction",
                mutate = { it.ampKeyed.also { _ -> it.ampKeyed = false } },
                rollback = { state, previous -> state.ampKeyed = previous },
            )
        }
    }

    private fun waitForSequencerDelay(s: Session, delayMs: Int, operation: String) {
        if (delayMs == 0) return
        try {
            Thread.sleep(delayMs.toLong())
        } catch (failure: InterruptedException) {
            Thread.currentThread().interrupt()
            retireAfterControlFailure(s, operation)
            throw IllegalStateException("$operation was interrupted before terminal write", failure)
        }
    }

    override fun setTxDrive(level: Int) {
        require(level in 0..255) { "TX drive $level is outside 0..255" }
        applyExactControlState(
            c0 = 8,
            operation = "TX-drive transaction",
            mutate = { it.txDrive.also { _ -> it.txDrive = level } },
            rollback = { state, previous -> state.txDrive = previous },
        )
    }

    override fun setPaEnabled(on: Boolean) {
        require(!usesClassicCodec) {
            "${profile.displayName} has no host-controlled PA-enable bit"
        }
        applyExactControlState(
            c0 = 8,
            operation = "PA-enable transaction",
            mutate = { it.paEnabled.also { _ -> it.paEnabled = on } },
            rollback = { state, previous -> state.paEnabled = previous },
        )
    }

    /** Enable VNA (antenna-analyzer) sweep mode — addr 0x09 data bit 23. */
    fun setVnaMode(on: Boolean) {
        require(!usesClassicCodec) { "${profile.displayName} has no HL2 VNA-mode bit" }
        applyExactControlState(
            c0 = 8,
            operation = "VNA-mode transaction",
            mutate = { it.vnaMode.also { _ -> it.vnaMode = on } },
            rollback = { state, previous -> state.vnaMode = previous },
        )
    }

    /** VNA sweep point count — addr 0x09 data[15:0] (radio.v). */
    fun setVnaCount(n: Int) {
        require(n in 1..65_535) { "VNA point count $n is outside 1..65535" }
        require(!usesClassicCodec) { "${profile.displayName} has no HL2 VNA-count register" }
        applyExactControlState(
            c0 = 8,
            operation = "VNA-count transaction",
            mutate = { it.vnaCount.also { _ -> it.vnaCount = n } },
            rollback = { state, previous -> state.vnaCount = previous },
        )
    }

    private val oneShotQueue = ArrayDeque<Pair<Int, Int>>()

    /** Queue a raw one-shot register write (fires once, next control frame). */
    fun writeRegister(addr: Int, data: Int) {
        synchronized(stateLock) { oneShotQueue.addLast(addr to data) }
        nudge()
    }

    /**
     * I2C passthrough write (i2c_bus2.v): bus 2 = addr 0x3D, bus 1 = 0x3C.
     * data[31:25]=0x03 (magic), [24]=read, [22:16]=i2c device, [15:8]=reg,
     * [7:0]=value. External tuners/filter boards hang off these buses.
     */
    fun i2cWrite(bus2: Boolean, device: Int, reg: Int, value: Int) {
        val word = Hl2Protocol.i2cPassthroughWord(read = false, device = device, reg = reg, value = value)
        writeRegister(if (bus2) 0x3D else 0x3C, word)
    }

    /** PureSignal TX-feedback routing — addr 0x0A data[22]. */
    fun setPureSignal(on: Boolean) {
        require(profile.pureSignalSupported || !on) {
            "${profile.displayName} has no verified PureSignal feedback-DDC route"
        }
        require(
            !on || synchronized(stateLock) { state.diversityMode != DiversityMode.RX1_RX2 },
        ) { "RX2 cannot be PureSignal feedback while RX1/RX2 diversity is active" }
        val s = requireControlSession()
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) {
                state.pureSignal.also { state.pureSignal = on }
            }
            try {
                sendExactControlBankLocked(s, 10)
            } catch (failure: Exception) {
                synchronized(stateLock) { state.pureSignal = previous }
                retireAfterControlFailure(s, "PureSignal route transaction")
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    // ========================================================================
    // N2ADR IO board (Pico I2C slave 0x1D on bus 2)
    // ========================================================================

    private val ioLock = Any()
    private var ioEnabled = false
    private var ioRfInput = 0            // REG_RF_INPUTS: 0 = internal, 1 = J9, 2 = J9 RX / J10 TX
    private var ioOpMode = -1            // REG_OP_MODE (Thetis mode codes), −1 = not yet known
    private var ioNeedsReset = false     // write REG_CONTROL=1 before the next push
    private var ioResetDone = false      // the register reset already went out THIS session
    private var ioNeutralPending = false // the board is owed a REG_RF_INPUTS=0 write
    private var ioSentTxFreq = -1L
    private val ioSentFcodes = IntArray(IO_FCODE_SLOTS) { -1 }
    private var ioSentOpMode = -1
    private var ioSentRfInput = -1
    private var ioLastPushNs = 0L

    /**
     * Enable the IO-board protocol: the board's Pico firmware selects PA
     * filters / band outputs from the frequencies the HOST writes into its
     * registers — it never looks at the OC pins. While enabled the client
     * mirrors its own tune state into the board (TX frequency in regs 0–4
     * with the LSB write as commit trigger, per-receiver frequency codes in
     * regs 13–24, operating mode in reg 32), throttled to one batch per
     * 500 ms so the I2C passthrough never crowds out C&C traffic. [opMode]
     * uses the Thetis mode codes (0=LSB 1=USB … 9=DIGL); [rfInput] is the
     * REG_RF_INPUTS user setting for the external RX jack.
     */
    fun setIoBoard(enabled: Boolean, rfInput: Int, opMode: Int) {
        require(rfInput in 0..2) { "IO-board RF input $rfInput is outside 0..2" }
        require(opMode in setOf(-1, 0, 1, 4, 5, 6)) {
            "IO-board operating mode $opMode is not a Thetis-compatible value"
        }
        synchronized(ioLock) {
            // The register reset is a start-of-session action, not a
            // per-toggle one: it zeroes all 256 registers on the board,
            // including the antenna-tuner register that ATU firmware variants
            // drive as a live state machine. Toggling the switch during a tune
            // cycle would wipe the tuner's state mid-sequence.
            if (enabled && !ioResetDone) ioNeedsReset = true
            ioEnabled = enabled
            ioRfInput = rfInput
            ioOpMode = opMode
        }
        // Disabling stops the mirror for good — maybePushIoBoard() returns
        // early from here on — so this is the last chance to undo the RX
        // routing the board is still holding.
        if (!enabled && ioNeutralOwed()) sendIoNeutral()
        nudge()
    }

    /**
     * Does the board still owe a REG_RF_INPUTS = 0 write?
     *
     * The external-input modes (1 = J9 for RX, 2 = J9 on RX / J10 on TX) drive
     * the board's routing pins high and they LATCH there: only a
     * REG_RF_INPUTS = 0 write pulls them back to the internal RX input. The
     * register reset (REG_CONTROL = 1) clears the register file but leaves the
     * pins as they are, so it cannot be relied on to restore routing either.
     * Left alone, a board that ends a session in mode 1 or 2 keeps feeding RX
     * from J9 — the main antenna goes silent with nothing on screen to say so.
     *
     * Marking is sticky: if the write cannot be delivered now (no session, or
     * the link died) it stays owed and goes out on the next connect.
     */
    private fun ioNeutralOwed(): Boolean = synchronized(ioLock) {
        if (ioSentRfInput > 0) {
            ioSentRfInput = 0
            ioNeutralPending = true
        }
        ioNeutralPending
    }

    /**
     * Queue the neutral routing write on the live session. It rides the normal
     * one-shot register path, so the running control sender puts it on the
     * wire within a frame. With no session there is nothing to write to and
     * the debt stays recorded for the next connect.
     */
    private fun sendIoNeutral() {
        if (sessionRef.get() == null) return
        i2cWrite(bus2 = true, device = IoBoard.ADDRESS, reg = IoBoard.REG_RF_INPUTS, value = 0)
        synchronized(ioLock) { ioNeutralPending = false }
    }

    /** Is a queued register write still waiting for a control frame to carry it? */
    private fun ioWritePending(): Boolean =
        synchronized(stateLock) { state.oneShotAddr >= 0 || oneShotQueue.isNotEmpty() }

    /**
     * Mirror tune state into the IO board when it drifted from what was last
     * written. Registers are static on the Pico, so only deltas go out; the
     * TX-frequency LSB (reg 4) is always rewritten on a change because its
     * write is what latches the new value in the firmware's IRQ handler.
     */
    private fun maybePushIoBoard() {
        val now = System.nanoTime()
        val writes = ArrayList<Pair<Int, Int>>(8)
        synchronized(ioLock) {
            if (!ioEnabled || now - ioLastPushNs < IO_PUSH_PERIOD_NS) return
            if (ioNeedsReset) {
                // Power-up registers may hold a previous session's data.
                writes.add(IoBoard.REG_CONTROL to 1)
                ioSentTxFreq = -1L
                ioSentFcodes.fill(-1)
                ioSentOpMode = -1
                ioSentRfInput = -1
                ioNeedsReset = false
                ioResetDone = true
            }
            val (txHz, fcodes) = synchronized(stateLock) {
                val codes = IntArray(IO_FCODE_SLOTS) { rx ->
                    if (rx < state.receiverCount) IoBoard.fcode(state.rxFreqHz[rx]) else 0
                }
                state.txFreqHz to codes
            }
            if (txHz != ioSentTxFreq) {
                for (b in 4 downTo 1) {
                    val v = ((txHz shr (8 * b)) and 0xFF).toInt()
                    if (ioSentTxFreq < 0 || v != ((ioSentTxFreq shr (8 * b)) and 0xFF).toInt()) {
                        writes.add((IoBoard.REG_TX_FREQ_BYTE4 + (4 - b)) to v)
                    }
                }
                writes.add(IoBoard.REG_TX_FREQ_BYTE0 to (txHz and 0xFF).toInt())
                ioSentTxFreq = txHz
            }
            for (rx in 0 until IO_FCODE_SLOTS) {
                if (fcodes[rx] != ioSentFcodes[rx]) {
                    writes.add((IoBoard.REG_FCODE_RX1 + rx) to fcodes[rx])
                    ioSentFcodes[rx] = fcodes[rx]
                }
            }
            if (ioRfInput != ioSentRfInput) {
                writes.add(IoBoard.REG_RF_INPUTS to ioRfInput)
                ioSentRfInput = ioRfInput
            }
            if (ioOpMode >= 0 && ioOpMode != ioSentOpMode) {
                writes.add(IoBoard.REG_OP_MODE to ioOpMode)
                ioSentOpMode = ioOpMode
            }
            if (writes.isEmpty()) return
            ioLastPushNs = now
        }
        for ((reg, value) in writes) i2cWrite(bus2 = true, device = IoBoard.ADDRESS, reg = reg, value = value)
    }

    /**
     * Gateware iambic keyer (addr 0x0B/0x0F/0x10): hardware-timed dits/dahs
     * from the paddle jack — zero-latency semi-breakin done in the FPGA.
     * [mode] 0=straight 1=iambic-A 2=iambic-B.
     */
    fun setCwKeyer(
        enabled: Boolean, wpm: Int, mode: Int, weight: Int,
        spacing: Boolean, reverse: Boolean, pttDelayMs: Int, hangMs: Int,
    ) {
        require(wpm in 1..60) { "CW speed $wpm is outside 1..60 WPM" }
        require(mode in 0..2) { "CW keyer mode $mode is outside 0..2" }
        require(weight in 10..90) { "CW weight $weight is outside 10..90" }
        require(pttDelayMs in 0..255) { "CW PTT delay $pttDelayMs is outside 0..255 ms" }
        require(hangMs in 0..1023) { "CW hang $hangMs is outside 0..1023 ms" }
        require(!usesClassicCodec) {
            "${profile.displayName} has no verified HL2 CW-keyer register layout"
        }
        val s = requireControlSession()
        synchronized(sendLock) {
            check(sessionRef.get() === s && s.running && !s.socket.isClosed) {
                "Protocol-1 control sender is not running"
            }
            val previous = synchronized(stateLock) {
                CwKeyerSnapshot(
                    state.cwKeyerEnabled,
                    state.cwSpeedWpm,
                    state.cwMode,
                    state.cwWeight,
                    state.cwSpacing,
                    state.cwReverse,
                    state.cwPttDelayMs,
                    state.cwHangMs,
                ).also {
                    state.cwKeyerEnabled = enabled
                    state.cwSpeedWpm = wpm
                    state.cwMode = mode
                    state.cwWeight = weight
                    state.cwSpacing = spacing
                    state.cwReverse = reverse
                    state.cwPttDelayMs = pttDelayMs
                    state.cwHangMs = hangMs
                }
            }
            try {
                // Settings occupy addr0B, addr0F and addr10 respectively.
                sendExactControlBankLocked(s, 10)
                sendExactControlBankLocked(s, 0x0E)
                sendExactControlBankLocked(s, 0x10)
            } catch (failure: Exception) {
                synchronized(stateLock) { previous.restore(state) }
                retireAfterControlFailure(s, "CW-keyer transaction")
                throw failure
            }
        }
        lastControlChangeNs = System.nanoTime()
    }

    private data class CwKeyerSnapshot(
        val enabled: Boolean,
        val speedWpm: Int,
        val mode: Int,
        val weight: Int,
        val spacing: Boolean,
        val reverse: Boolean,
        val pttDelayMs: Int,
        val hangMs: Int,
    ) {
        fun restore(state: Hl2Protocol.ControlState) {
            state.cwKeyerEnabled = enabled
            state.cwSpeedWpm = speedWpm
            state.cwMode = mode
            state.cwWeight = weight
            state.cwSpacing = spacing
            state.cwReverse = reverse
            state.cwPttDelayMs = pttDelayMs
            state.cwHangMs = hangMs
        }
    }

    /** Hold the T/R relay in receive even while keyed — addr 0x09 data bit 18. */
    fun setTrDisable(on: Boolean) {
        require(!usesClassicCodec) { "${profile.displayName} has no HL2 T/R-disable bit" }
        applyExactControlState(
            c0 = 8,
            operation = "T/R-disable transaction",
            mutate = { it.trDisable.also { _ -> it.trDisable = on } },
            rollback = { state, previous -> state.trDisable = previous },
        )
    }

    /**
     * Set the 7 open-collector outputs (addr 0x00 data[23:17]). The gateware
     * forwards them to the filter board over I2C (chip 0x20) for one-hot
     * low-/high-pass filter selection. [mask] bits 0..6; higher bits ignored.
     */
    /**
     * Band-data word on the OC pins; [txMask] (−1 = same as [mask]) is the
     * word while keyed — switched atomically with mox in the frame builder.
     */
    fun setOpenCollectorOutputs(mask: Int, txMask: Int = -1) {
        require(mask >= 0 && mask and 0x7F.inv() == 0) {
            "open-collector mask 0x${mask.toString(16)} exceeds 0x7f"
        }
        require(txMask == -1 || txMask >= 0 && txMask and 0x7F.inv() == 0) {
            "TX open-collector mask 0x${txMask.toString(16)} is not -1 or a 7-bit mask"
        }
        applyExactControlState(
            c0 = 0,
            operation = "open-collector transaction",
            mutate = {
                Pair(it.ocOutputs, it.ocOutputsTx).also { _ ->
                    it.ocOutputs = mask
                    it.ocOutputsTx = txMask
                }
            },
            rollback = { state, previous ->
                state.ocOutputs = previous.first
                state.ocOutputsTx = previous.second
            },
        )
    }

    fun getOpenCollectorOutputs(): Int = synchronized(stateLock) { state.ocOutputs }

    override fun isTransmitting(): Boolean = synchronized(stateLock) { state.mox || state.ampKeyed }

    /** Queue interleaved transmit IQ (`i0,q0,…` in `[-1,1]`, 48 kSps). Drop-oldest on overflow. */
    override fun submitTxIq(iq: FloatArray) {
        synchronized(txLock) { txQueue.write(iq) }   // ring drops oldest itself
    }

    private fun updateFlushThreshold() {
        flushPairs = maxOf(800, synchronized(stateLock) { state.sampleRate } / 50)
    }
}
