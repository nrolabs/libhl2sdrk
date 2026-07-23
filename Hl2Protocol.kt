/*
 * libhl2sdrk - Hermes-Lite 2 driver, openHPSDR "Protocol 1" (Metis) codec.
 *
 * Pure Kotlin, free of any Android dependency, so the wire format can be
 * unit-tested on the JVM and reused outside the app. Ported faithfully from the
 * reference driver (Quisk) and the openHPSDR Protocol 1 specification / the
 * Hermes-Lite 2 register documentation:
 *   https://github.com/softerhardware/Hermes-Lite2/blob/master/software/hl2setup/hl2.cxx
 *   https://github.com/softerhardware/Hermes-Lite2/wiki
 *
 * Copyright (C) 2026 Isak Ruas <isakruas@gmail.com>. GPL v2 or later.
 */

package com.isaklab.libhl2sdrk

/**
 * Frame codec for the Hermes-Lite 2. Builds host→board control/transmit frames
 * and parses board→host RX frames. Holds no state; the caller owns the socket,
 * the rotating C0 index, the sequence counter and the TX sample source.
 */
object Hl2Protocol {

    const val PORT = 1024
    const val FRAME = 1032

    /** The two sub-frame sync/C0 offsets — hoisted so parseRxFrame allocates
     *  no per-frame array (it runs ~380×/s). */
    private val SUBFRAME_BASES = intArrayOf(11, 523)
    const val SAMPLES_PER_SUBFRAME = 63
    const val TX_RATE = 48000                 // downstream (mic/TX) is always 48 kSps

    /** Sample-rate "speed" field (C0=0, C1[1:0]). */
    val RATE_TO_SPEED = mapOf(48000 to 0, 96000 to 1, 192000 to 2, 384000 to 3)

    /**
     * Filter-board one-hot select bits driven on the open-collector outputs
     * (addr 0x00 C2[7:1]). Values are the reference `FILTER_*` masks from the
     * Hermes-Lite 2 `hl2setup/hl2.h`.
     */
    object Filter {
        const val OFF = 0x00
        const val LPF_160M = 0x01
        const val LPF_80M = 0x02
        const val LPF_60_40M = 0x04
        const val LPF_30_20M = 0x08
        const val LPF_17_15M = 0x10
        const val LPF_12_10M = 0x20
        const val HPF = 0x40
    }

    /** Programmable radio state carried in the C0 banks. */
    class ControlState {
        var rxFreqHz = 7_100_000L        // receiver 1 NCO
        var rx2FreqHz = 7_100_000L       // receiver 2 NCO (when receiverCount == 2)
        var txFreqHz = 7_100_000L
        var sampleRate = 48000
        var lnaGainDb = 20
        var txDrive = 0
        var paEnabled = false
        var mox = false
        var receiverCount = 1            // 1 or 2 hardware receivers
        var vnaMode = false              // addr 0x09 data bit 23 — antenna-analyzer sweep
        var trDisable = false            // addr 0x09 data bit 18 — hold the T/R relay in RX
        var ocOutputs = 0                // addr 0x00 data[23:17] — 7 open-collector outputs (RX)
        /**
         * OC word while transmitting. −1 mirrors [ocOutputs]. Selected inside
         * the frame builder from the SAME mox flag that keys the radio, so
         * the switch is atomic per frame — exactly the reference behaviour
         * (hl2.cxx: `if (hermes_key_down) sendbuf[13] = filter_tx << 1`).
         * External linears key their band relays off these pins; a word that
         * lags the keying edge hot-switches relays under RF.
         */
        var ocOutputsTx = -1
        var pttHang = 4                  // addr 0x17 C3[4:0] — PTT hang time (gateware default)
        var txLatencyMs = 10             // addr 0x17 C4[6:0] — TX buffer latency in ms
        var resetOnDisconnect = true     // addr 0x3A C4 bit 0 — free the board on link loss
        /**
         * Classic Protocol-1 board (Hermes/Angelia/Orion families) instead of
         * an HL2: gain is the documented step attenuator (addr 0x0A C4 bit5
         * enable + 5-bit dB) rather than the HL2 extended-gain encoding, and
         * the HL2-specific addr 0x09 C2 bits (VNA/PA/T-R) stay clear — the
         * classic firmware assigns that byte to other functions.
         */
        var classicBoard = false
    }

    /** RX sample slot size in bytes for [nRx] receivers: I(3)+Q(3) per RX + mic(2). */
    fun slotBytes(nRx: Int) = 6 * nRx + 2

    /** IQ sample pairs per 512-byte sub-frame for [nRx] receivers (504 payload bytes). */
    fun samplesPerSubframe(nRx: Int) = 504 / slotBytes(nRx)

    data class Telemetry(
        val temperatureC: Double,
        val paCurrentA: Double,
        val forwardPower: Int,
        val reversePower: Int,
        val hasData: Boolean,
        /** Board supply voltage (AIN6), or 0.0 until a 0x18 frame arrives. */
        val supplyVolts: Double = 0.0,
        val hasSupplyVolts: Boolean = false,
        // The board rotates its telemetry banks, so any single frame carries
        // only a subset — consumers must gate each field on its validity flag
        // and hold the last known value, never read an absent one (it decodes
        // to nonsense like -50 °C).
        val hasTemperature: Boolean = false,
        val hasCurrent: Boolean = false,
        // Forward power rides the temperature bank and reverse the current
        // bank on the HL2; other radios may deliver them independently.
        val hasFwdPower: Boolean = false,
        val hasRevPower: Boolean = false,
        // RX-frame status bits (every frame): the radio's hardware key/PTT
        // inputs (C0 bit0=PTT, bit1=DASH, bit2=DOT — the layout Thetis
        // decodes) and the ADC clip flag (bank-0 C1 bit0).
        val keyPtt: Boolean = false,
        val keyDash: Boolean = false,
        val keyDot: Boolean = false,
        val adcOverflow: Boolean = false,
        val hasAdcOverflow: Boolean = false
    )

    // ---- host -> board ------------------------------------------------------

    fun discoveryRequest(): ByteArray =
        ByteArray(64).also { it[0] = 0xEF.toByte(); it[1] = 0xFE.toByte(); it[2] = 0x02 }

    fun startStop(start: Boolean): ByteArray =
        ByteArray(64).also {
            it[0] = 0xEF.toByte(); it[1] = 0xFE.toByte(); it[2] = 0x04
            it[3] = if (start) 0x01 else 0x00
        }

    /**
     * Builds one 1032-byte control/transmit frame for bank pair [c0Index]
     * (0,2,4,6,8,10,0x16,0x3A). [pullTxSample] is called once per transmit sample slot
     * (126×) and returns packed `(i16 shl 16) or (q16 and 0xFFFF)` or `null`
     * for silence; it is only consulted while `state.mox` is set.
     */
    fun buildControlFrame(
        seq: Long,
        c0Index: Int,
        state: ControlState,
        pullTxSample: (() -> Int?)?
    ): ByteArray {
        val buf = ByteArray(FRAME)
        buf[0] = 0xEF.toByte(); buf[1] = 0xFE.toByte(); buf[2] = 0x01; buf[3] = 0x02
        putBE32(buf, 4, seq)

        val moxBit = if (state.mox) 1 else 0
        buf[8] = 0x7F; buf[9] = 0x7F; buf[10] = 0x7F
        buf[520] = 0x7F; buf[521] = 0x7F; buf[522] = 0x7F
        buf[11] = ((c0Index shl 1) or moxBit).toByte()
        buf[523] = (((c0Index + 1) shl 1) or moxBit).toByte()

        when (c0Index) {
            0 -> {                                       // addr0 config + addr1 TX freq
                buf[12] = (RATE_TO_SPEED[state.sampleRate] ?: 0).toByte()  // C1 speed
                // C2 = data[23:16]: the 7 open-collector outputs sit in data[23:17],
                // i.e. C2[7:1]; the gateware forwards them to the filter board (I2C 0x20)
                // for one-hot low-/high-pass filter selection. The TX word is
                // chosen by the same mox flag that sets the C0 keying bit.
                val oc = if (state.mox && state.ocOutputsTx >= 0) state.ocOutputsTx
                         else state.ocOutputs
                buf[13] = ((oc and 0x7F) shl 1).toByte()
                // C4: duplex (bit2) + number of receivers - 1 (bits 6:3)
                buf[15] = (0x04 or (((state.receiverCount - 1) and 0x07) shl 3)).toByte()
                putBE32(buf, 524, state.txFreqHz)
            }
            2 -> {                                       // addr2 RX1 freq + addr3 RX2 freq
                putBE32(buf, 12, state.rxFreqHz)
                putBE32(buf, 524, state.rx2FreqHz)
            }
            8 -> {                                        // addr9 drive + PA/flags (C2)
                buf[524] = state.txDrive.toByte()         // C1 = data[31:24] = TX drive
                var c2 = 0                                 // C2 = data[23:16]
                if (!state.classicBoard) {                 // HL2 gateware extensions only
                    if (state.vnaMode) c2 = c2 or 0x80    // bit23 — VNA sweep mode
                    if (state.paEnabled) c2 = c2 or 0x08  // bit19 — PA enable
                    if (state.trDisable) c2 = c2 or 0x04  // bit18 — disable T/R relay
                }
                // SAFETY (classic ANAN incl. Orion MkII): AlexManEnable (this
                // byte, manual-filter enable) MUST stay clear. With it clear
                // the firmware auto-selects BOTH the LPF and the MkII BPF2
                // HPF board from the frequency word — the same safe path
                // PowerSDR uses by default. Setting it without a verified
                // per-band HPF/LPF table would key TX through an unknown
                // filter. Do not add manual Alex words here without
                // bench-verifying the table on the exact board at low power.
                buf[525] = c2.toByte()
            }
            10 -> buf[15] =
                if (state.classicBoard) {
                    // Classic step attenuator: bit5 enable + 0..31 dB. The
                    // shared −12..+48 dB gain scale maps onto attenuation.
                    val att = ((48 - state.lnaGainDb.coerceIn(-12, 48)) * 31) / 60
                    (0x20 or (att and 0x1F)).toByte()
                } else {
                    (((state.lnaGainDb + 12) and 0x3F) or 0x40).toByte()  // addr0A HL2 LNA
                }
            0x16 -> {                                    // addr0x16 (unused) + addr0x17 TX buffer
                buf[526] = (state.pttHang and 0x1F).toByte()      // C3 = PTT hang
                buf[527] = (state.txLatencyMs and 0x7F).toByte()  // C4 = TX latency ms
            }
            0x3A -> buf[15] = if (state.resetOnDisconnect) 1 else 0  // addr0x3A C4 bit0
        }

        fillTxSamples(buf, 16, state.mox, pullTxSample)
        fillTxSamples(buf, 528, state.mox, pullTxSample)
        return buf
    }

    private fun fillTxSamples(buf: ByteArray, start: Int, transmitting: Boolean, pull: (() -> Int?)?) {
        var p = start
        for (k in 0 until SAMPLES_PER_SUBFRAME) {
            var i16 = 0; var q16 = 0
            if (transmitting && pull != null) {
                val packed = pull()
                if (packed != null) { i16 = packed shr 16; q16 = (packed shl 16) shr 16 }
            }
            // Mirror of the RX wire order: the DUC expects the IMAGINARY part
            // in the first 16-bit slot and the REAL part in the second, or the
            // transmitted sideband comes out mirrored on the air.
            buf[p + 4] = (q16 shr 8).toByte(); buf[p + 5] = q16.toByte()
            buf[p + 6] = (i16 shr 8).toByte(); buf[p + 7] = i16.toByte()
            p += 8
        }
    }

    // ---- board -> host ------------------------------------------------------

    fun isRxFrame(buf: ByteArray, length: Int): Boolean =
        length == FRAME && buf[0] == 0xEF.toByte() && buf[1] == 0xFE.toByte() &&
                buf[2] == 0x01.toByte() && buf[3] == 0x06.toByte()

    /**
     * Parses a board→host RX frame for [receiverCount] receivers, calling
     * [onSample]`(rx, i, q)` for every IQ pair of every receiver (I, Q in
     * `[-1,1]`) and returning decoded telemetry. The sample slot is
     * `6*receiverCount + 2` bytes (I+Q per RX, then a shared mic word).
     * Sub-frames with a bad sync word are skipped.
     */
    /**
     * Per-sample sink with PRIMITIVE parameters. A plain
     * `(Int, Float, Float) -> Unit` Kotlin function type boxes all three
     * arguments on every call — at 48 kSps that churned ~140k boxes/s and the
     * resulting GC storm popped the audio. A fun interface with a primitive
     * signature compiles to `onSample(int, float, float)` — zero boxing.
     */
    fun interface SampleSink {
        fun onSample(rx: Int, i: Float, q: Float)
    }

    fun parseRxFrame(
        buf: ByteArray,
        receiverCount: Int,
        onSample: SampleSink
    ): Telemetry {
        var tempAcc = 0; var fwd = 0; var rev = 0; var curAcc = 0; var voltAcc = 0
        var tCount = 0; var cCount = 0; var vCount = 0
        var keyPtt = false; var keyDash = false; var keyDot = false
        var ovf = false; var sawBank0 = false
        val slot = slotBytes(receiverCount)
        val nSamp = samplesPerSubframe(receiverCount)

        for (base in SUBFRAME_BASES) {
            if (buf[base - 3] != 0x7F.toByte() || buf[base - 2] != 0x7F.toByte() ||
                buf[base - 1] != 0x7F.toByte()
            ) continue

            val c0 = buf[base].toInt() and 0xFF
            keyPtt = keyPtt || (c0 and 0x01) != 0
            keyDash = keyDash || (c0 and 0x02) != 0
            keyDot = keyDot || (c0 and 0x04) != 0
            if (c0 shr 3 == 0) {
                sawBank0 = true
                ovf = ovf || (buf[base + 1].toInt() and 0x01) != 0
            }
            when (c0 shr 3) {
                1 -> { tempAcc += be16(buf, base + 1); fwd += be16(buf, base + 3); tCount++ }
                2 -> { rev += be16(buf, base + 1); curAcc += be16(buf, base + 3); cCount++ }
                3 -> { voltAcc += be16(buf, base + 3); vCount++ }   // C3-4 = AIN6 supply
            }

            var idx = base + 5
            for (k in 0 until nSamp) {
                for (rx in 0 until receiverCount) {
                    val o = idx + rx * 6
                    // Wire order is IMAGINARY first, REAL second (hl2.cxx reads
                    // ximag then xreal): with the standard z = i + jq math the
                    // first word must land in q, or the spectrum mirrors and
                    // USB/LSB swap on real hardware.
                    onSample.onSample(rx, s24(buf, o + 3) / 8_388_608f, s24(buf, o) / 8_388_608f)
                }
                idx += slot
            }
        }

        val hasData = tCount > 0 || cCount > 0 || vCount > 0
        val tRaw = if (tCount > 0) tempAcc.toDouble() / tCount else 0.0
        val cRaw = if (cCount > 0) curAcc.toDouble() / cCount else 0.0
        val tempC = (3.26 * (tRaw / 4096.0) - 0.5) / 0.01
        var paCur = (3.26 * (cRaw / 4096.0)) / 50.0 / 0.04
        paCur /= (1000.0 / 1270.0)
        // Bank 3 is the always-zero `debug` word on STOCK HL2 gateware
        // (control.v: "Unused in HL") — a supply reading only exists on
        // extended gateware (MI0BOT-style AIN6 through a (4.7k+820)/820
        // divider, 3.3 V full scale). A zero raw value therefore means "no
        // voltage channel", never 0 V, and must not surface as data.
        val vRaw = if (vCount > 0) voltAcc.toDouble() / vCount else 0.0
        val volts = vRaw / 4095.0 * 3.3 * ((4.7 + 0.82) / 0.82)
        return Telemetry(
            temperatureC = tempC,
            paCurrentA = paCur,
            forwardPower = if (tCount > 0) fwd / tCount else 0,
            reversePower = if (cCount > 0) rev / cCount else 0,
            hasData = hasData,
            supplyVolts = volts,
            hasSupplyVolts = vCount > 0 && vRaw > 0.0,
            hasTemperature = tCount > 0,
            hasCurrent = cCount > 0,
            hasFwdPower = tCount > 0,
            hasRevPower = cCount > 0,
            keyPtt = keyPtt,
            keyDash = keyDash,
            keyDot = keyDot,
            adcOverflow = ovf,
            hasAdcOverflow = sawBank0
        )
    }

    /** True if a datagram is a Metis discovery reply (board present). */
    fun isDiscoveryReply(buf: ByteArray, length: Int): Boolean =
        length > 32 && buf[0] == 0xEF.toByte() && buf[1] == 0xFE.toByte()

    fun boardIdOf(buf: ByteArray): Int = buf[10].toInt() and 0xFF

    // ---- byte helpers -------------------------------------------------------

    fun be16(b: ByteArray, o: Int) = ((b[o].toInt() and 0xFF) shl 8) or (b[o + 1].toInt() and 0xFF)

    fun s24(b: ByteArray, o: Int): Int {
        val v = ((b[o].toInt() and 0xFF) shl 16) or
                ((b[o + 1].toInt() and 0xFF) shl 8) or
                (b[o + 2].toInt() and 0xFF)
        return if (v and 0x800000 != 0) v - 0x1000000 else v
    }

    fun putBE32(b: ByteArray, o: Int, v: Long) {
        b[o] = (v ushr 24).toByte(); b[o + 1] = (v ushr 16).toByte()
        b[o + 2] = (v ushr 8).toByte(); b[o + 3] = v.toByte()
    }

    /** Pack a float in `[-1,1]` to a signed 16-bit sample (low 16 bits used). */
    fun toS16(f: Float): Int =
        (f.coerceIn(-1f, 1f) * 32767f).toInt().coerceIn(-32768, 32767) and 0xFFFF
}
