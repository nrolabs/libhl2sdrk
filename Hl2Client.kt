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

import android.util.Log
import com.isaklab.isdrdrivers.core.FFTProcessor
import kotlinx.coroutines.*
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketTimeoutException

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
    private val onTelemetry: ((Hl2Protocol.Telemetry) -> Unit)? = null,
    /** Board UDP port — override only if the firmware default (1024) was changed. */
    private val port: Int = Hl2Protocol.PORT,
    /**
     * Classic Protocol-1 board (Hermes/Angelia/Orion families): switches the
     * codec to the classic attenuator encoding, suppresses HL2-only C2 bits,
     * and drops the temperature/current telemetry claims (those AIN slots
     * hold other channels on classic boards).
     */
    private val classicBoard: Boolean = false,
) {
    companion object {
        const val BROADCAST = "255.255.255.255"
        private const val TAG = "Hl2Client"
        private val EMPTY_SPECTRUM = FloatArray(0)

        // Round-robin of C0 bank pairs. Beyond the classic Hermes banks, the
        // HL2 needs 0x16/0x17 (TX buffer latency + PTT hang — without it the
        // gateware default can drop the first TX syllable) and 0x3A (reset on
        // disconnect, so a dead client session frees the board).
        private val C0_SEQUENCE = intArrayOf(0, 2, 4, 6, 8, 10, 0x16, 0x3A)

        // Classic Protocol-1 firmware only defines the documented banks —
        // the reference clients (hl2.cxx, PowerSDR) rotate 0..10 and nothing
        // else. Sending HL2-extension addresses to old firmware is undefined
        // behaviour, so the rotation excludes them.
        private val C0_SEQUENCE_CLASSIC = intArrayOf(0, 2, 4, 6, 8, 10)
    }

    /**
     * When false, RX blocks skip the FFT and are delivered with an empty
     * spectrum array — the host has no visible spectrum consumer. Audio/IQ
     * delivery is unaffected.
     */
    @Volatile var spectrumEnabled: Boolean = true

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var receiveJob: Job? = null
    private var txSenderJob: Job? = null
    private var socket: DatagramSocket? = null
    private var board: InetAddress? = null
    @Volatile private var running = false

    private var fft: FFTProcessor? = null

    private val stateLock = Any()
    private val state = Hl2Protocol.ControlState().also { it.classicBoard = classicBoard }
    private var txSeq = 0L
    private var c0Index = 0

    // RX accumulation (delivered as display-sized blocks).
    private var lastSpectrum: FloatArray? = null
    private var lastFftTimeMs = 0L
    private val fftIntervalMs = 80L
    private var accum = FloatArray(0)
    private var accumPairs = 0
    private var flushPairs = 800

    // TX sample queue: interleaved I/Q in [-1,1] at 48 kSps, drop-oldest.
    private val txQueue = ArrayDeque<Float>()
    private val txLock = Any()
    private val maxTxQueue = Hl2Protocol.TX_RATE * 4

    // Which hardware receiver feeds the spectrum/audio when 2 are configured.
    @Volatile private var activeReceiver = 0

    // Last-seen RX-frame status bits: the key/PTT inputs and ADC clip flag
    // are EDGE information — telemetry must fire on every change even when
    // the frame carries no AIN bank data (hasData false).
    private var lastKeyState = 0

    // Reused per-frame IQ sink to avoid a lambda allocation on the hot path;
    // only the active receiver's samples are accumulated for display/audio.
    private val sampleSink = Hl2Protocol.SampleSink { rx, i, q ->
        if (rx == activeReceiver) appendSample(i, q)
    }
    private val txPull: () -> Int? = {
        synchronized(txLock) {
            if (txQueue.size >= 2) {
                val i = Hl2Protocol.toS16(txQueue.removeFirst())
                val q = Hl2Protocol.toS16(txQueue.removeFirst())
                (i shl 16) or (q and 0xFFFF)
            } else null
        }
    }

    // ========================================================================
    // Connection lifecycle
    // ========================================================================

    suspend fun connect(): Boolean = withContext(Dispatchers.IO) {
        try {
            onConnectionStatusChanged(false, "Discovering…")
            board = if (host == BROADCAST) discover() else InetAddress.getByName(host)
            if (board == null) {
                onConnectionStatusChanged(false, "No Hermes-Lite 2 found")
                return@withContext false
            }
            val s = DatagramSocket()
            s.soTimeout = 1000
            s.connect(board, port)
            socket = s
            fft = FFTProcessor(800)
            updateFlushThreshold()

            sendStartStop(false); delay(20)
            sendStartStop(false); delay(20)
            sendStartStop(true)
            running = true
            onConnectionStatusChanged(true, "Connected")
            startReceiving()
            startTxSender()
            true
        } catch (e: Exception) {
            Log.e(TAG, "connect failed: ${e.message}")
            onConnectionStatusChanged(false, "Connection failed")
            disconnect()
            false
        }
    }

    fun disconnect() {
        scope.launch {
            running = false
            try { sendStartStop(false) } catch (_: Exception) {}
            receiveJob?.cancel()
            txSenderJob?.cancel()
            socket?.close()
            socket = null
            onConnectionStatusChanged(false, "Disconnected")
        }
    }

    private fun discover(): InetAddress? {
        val ds = DatagramSocket()
        try {
            ds.broadcast = true
            ds.soTimeout = 500
            val req = Hl2Protocol.discoveryRequest()
            val dst = InetAddress.getByName(BROADCAST)
            val reply = ByteArray(64)
            repeat(4) {
                ds.send(DatagramPacket(req, req.size, dst, port))
                try {
                    val p = DatagramPacket(reply, reply.size)
                    ds.receive(p)
                    if (Hl2Protocol.isDiscoveryReply(reply, p.length)) {
                        Log.i(TAG, "found HL2 board_id=0x%02x at %s"
                            .format(Hl2Protocol.boardIdOf(reply), p.address.hostAddress))
                        return p.address
                    }
                } catch (_: SocketTimeoutException) { /* retry */ }
            }
        } finally {
            ds.close()
        }
        return null
    }

    // ========================================================================
    // RX
    // ========================================================================

    private fun startReceiving() {
        receiveJob = scope.launch {
            // Raise this thread to audio priority: it feeds the audio pipeline,
            // and on a low-end phone the spectrum/waterfall rendering otherwise
            // preempted it, delaying block delivery and popping the audio. The
            // receive loop owns its thread (only suspends on socket.receive).
            try {
                android.os.Process.setThreadPriority(
                    android.os.Process.THREAD_PRIORITY_URGENT_AUDIO
                )
            } catch (_: Throwable) {}
            val buf = ByteArray(2048)
            val packet = DatagramPacket(buf, buf.size)
            var noData = 0
            while (running && isActive) {
                try {
                    socket!!.receive(packet)
                    noData = 0
                    if (Hl2Protocol.isRxFrame(buf, packet.length)) {
                        val nRx = synchronized(stateLock) { state.receiverCount }
                        var telem = Hl2Protocol.parseRxFrame(buf, nRx, sampleSink)
                        // Classic boards put exciter power / another AIN in
                        // the HL2's temperature/current slots — never claim
                        // those channels for them (fwd/rev/volts stay valid).
                        if (classicBoard) telem = telem.copy(
                            hasTemperature = false, hasCurrent = false,
                        )
                        if (accumPairs >= flushPairs) flushRx()
                        val keyState = (if (telem.keyPtt) 1 else 0) or
                            (if (telem.keyDash) 2 else 0) or
                            (if (telem.keyDot) 4 else 0) or
                            (if (telem.adcOverflow) 8 else 0)
                        if (telem.hasData || keyState != lastKeyState) {
                            lastKeyState = keyState
                            onTelemetry?.invoke(telem)
                        }
                    }
                } catch (e: SocketTimeoutException) {
                    if (++noData > 5) {
                        try { sendStartStop(true) } catch (_: Exception) {}
                        if (noData > 15) {
                            onConnectionStatusChanged(false, "Timeout")
                            running = false
                        }
                    }
                } catch (e: Exception) {
                    if (running) {
                        Log.e(TAG, "rx error: ${e.message}")
                        onConnectionStatusChanged(false, "Error")
                        running = false
                    }
                }
            }
        }
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
    private fun startTxSender() {
        txSenderJob = scope.launch {
            val floatsPerFrame = 2 * Hl2Protocol.SAMPLES_PER_SUBFRAME * 2  // 126 pairs × 2 floats
            val pairsPerFrame = 2 * Hl2Protocol.SAMPLES_PER_SUBFRAME      // samples per frame
            var txStartNs = 0L
            var framesSent = 0L
            while (running && isActive) {
                val transmitting = synchronized(stateLock) { state.mox }
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
                        while (framesSent < targetFrames && burst < 16) {
                            val ready = synchronized(txLock) { txQueue.size >= floatsPerFrame }
                            if (!ready) break
                            sendControlFrame()
                            framesSent++
                            burst++
                        }
                    }
                    delay(1)
                } else {
                    txStartNs = 0L
                    sendControlFrame()
                    delay(3)
                }
            }
        }
    }

    private fun appendSample(i: Float, q: Float) {
        val need = (accumPairs + 1) * 2
        if (accum.size < need) accum = accum.copyOf(maxOf(need, flushPairs * 2))
        accum[accumPairs * 2] = i
        accum[accumPairs * 2 + 1] = q
        accumPairs++
    }

    private fun flushRx() {
        val block = accum.copyOf(accumPairs * 2)
        accumPairs = 0
        // With every spectrum consumer hidden the FFT is pure waste on a phone
        // CPU — deliver the IQ with an empty spectrum instead (audio must never
        // depend on the FFT path).
        if (!spectrumEnabled) {
            onDataReceived(EMPTY_SPECTRUM, block)
            return
        }
        // The display renders ~10 fps: recompute the FFT on that cadence and
        // re-serve the cached spectrum in between (audio never waits on it).
        val now = System.currentTimeMillis()
        val spectrum = if (lastSpectrum == null || now - lastFftTimeMs >= fftIntervalMs) {
            lastFftTimeMs = now
            fft?.computePowerSpectrum(block, block.size / 2)?.also { lastSpectrum = it }
        } else {
            lastSpectrum
        } ?: return
        onDataReceived(spectrum, block)
    }

    // ========================================================================
    // TX / control
    // ========================================================================

    private fun sendStartStop(start: Boolean) {
        val s = socket ?: return
        val b = Hl2Protocol.startStop(start)
        s.send(DatagramPacket(b, b.size))
    }

    private fun sendControlFrame() {
        val s = socket ?: return
        val sequence = if (classicBoard) C0_SEQUENCE_CLASSIC else C0_SEQUENCE
        val frame = synchronized(stateLock) {
            Hl2Protocol.buildControlFrame(txSeq++, sequence[c0Index % sequence.size], state, txPull)
        }
        c0Index = (c0Index + 1) % sequence.size
        s.send(DatagramPacket(frame, frame.size))
    }

    // ========================================================================
    // Public control API (mirrors the RTL clients, plus TX)
    // ========================================================================

    fun setFrequency(hz: Long) {
        synchronized(stateLock) { state.rxFreqHz = hz }
        fft?.resetSmoothing()
    }

    fun setSampleRate(hz: Int) {
        synchronized(stateLock) { state.sampleRate = hz }
        updateFlushThreshold()
        fft?.resetSmoothing()
    }

    fun setLnaGain(db: Int) { synchronized(stateLock) { state.lnaGainDb = db.coerceIn(-12, 48) } }

    /** Configure 1 or 2 hardware receivers. */
    fun setReceiverCount(n: Int) { synchronized(stateLock) { state.receiverCount = n.coerceIn(1, 2) } }

    /** Set receiver 2's frequency (used only when 2 receivers are configured). */
    fun setFrequency2(hz: Long) {
        synchronized(stateLock) { state.rx2FreqHz = hz }
    }

    /** Choose which receiver (0 or 1) drives the spectrum/waterfall and audio. */
    fun setActiveReceiver(index: Int) {
        activeReceiver = index.coerceIn(0, 1)
        fft?.resetSmoothing()
    }

    fun getActiveReceiver(): Int = activeReceiver

    fun getSampleRate(): Double = synchronized(stateLock) { state.sampleRate.toDouble() }

    fun setSmoothingFactor(alpha: Float) { fft?.setSmoothingFactor(alpha) }

    fun setTxFrequency(hz: Long) { synchronized(stateLock) { state.txFreqHz = hz } }

    fun setPtt(on: Boolean) {
        synchronized(stateLock) { state.mox = on }
        if (!on) synchronized(txLock) { txQueue.clear() }
    }

    fun setTxDrive(level: Int) { synchronized(stateLock) { state.txDrive = level.coerceIn(0, 255) } }

    fun setPaEnabled(on: Boolean) { synchronized(stateLock) { state.paEnabled = on } }

    /** Enable VNA (antenna-analyzer) sweep mode — addr 0x09 data bit 23. */
    fun setVnaMode(on: Boolean) { synchronized(stateLock) { state.vnaMode = on } }

    /** Hold the T/R relay in receive even while keyed — addr 0x09 data bit 18. */
    fun setTrDisable(on: Boolean) { synchronized(stateLock) { state.trDisable = on } }

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
        synchronized(stateLock) {
            state.ocOutputs = mask and 0x7F
            state.ocOutputsTx = if (txMask < 0) -1 else txMask and 0x7F
        }
    }

    fun getOpenCollectorOutputs(): Int = synchronized(stateLock) { state.ocOutputs }

    fun isTransmitting(): Boolean = synchronized(stateLock) { state.mox }

    /** Queue interleaved transmit IQ (`i0,q0,…` in `[-1,1]`, 48 kSps). Drop-oldest on overflow. */
    fun submitTxIq(iq: FloatArray) {
        synchronized(txLock) {
            for (v in iq) txQueue.addLast(v)
            while (txQueue.size > maxTxQueue) txQueue.removeFirst()
        }
    }

    private fun updateFlushThreshold() {
        flushPairs = maxOf(800, synchronized(stateLock) { state.sampleRate } / 50)
    }
}
