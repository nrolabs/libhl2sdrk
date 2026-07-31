/*
 * libhl2sdrk - N2ADR HL2 IO board register map and frequency-code codec.
 *
 * Protocol source: https://github.com/jimahlstrom/HL2IOBoard (MIT), files
 * i2c_registers.h and n2adr_lib/frequency_code.c.
 *
 * Copyright (C) 2026 Isak Ruas <isakruas@gmail.com>. GPL v2 or later.
 */

package com.isaklab.libhl2sdrk

import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.roundToLong

/**
 * Register map of the N2ADR HL2 IO board: a Pico listening as I2C slave
 * 0x1D on the HL2's second I2C bus. The board's firmware derives PA-filter /
 * band-decoder outputs (Yaesu BCD, band volts, amp-specific keying, …) from
 * the frequencies the host writes here — writes of one register + one byte
 * each, carried over the gateware I2C passthrough. It never looks at the
 * open-collector pins.
 */
object IoBoard {
    const val ADDRESS = 0x1D

    // TX frequency in Hz as five bytes, MSB in reg 0. Writing BYTE0 (reg 4)
    // is the commit trigger: the firmware assembles the value in its I2C
    // interrupt handler only on that write.
    const val REG_TX_FREQ_BYTE4 = 0
    const val REG_TX_FREQ_BYTE0 = 4
    /** Write 1 to reset every register to the power-up state. */
    const val REG_CONTROL = 5
    /** External RX input mode: 0 = internal, 1 = J9, 2 = J9 on RX / J10 on TX. */
    const val REG_RF_INPUTS = 11
    /** Frequency codes of receivers 1..12 (reg 13..24), 0 = slot unused. */
    const val REG_FCODE_RX1 = 13
    /** Operating mode, Thetis codes (0=LSB 1=USB 3=CWL 4=CWU 5=FM 6=AM 7=DIGU 9=DIGL). */
    const val REG_OP_MODE = 32

    /**
     * One-byte logarithmic frequency code: 0 = unspecified, monotonic in
     * frequency, precise enough to identify the band. This is the encoding
     * the board firmware expects in the RX frequency-code registers.
     */
    fun fcode(hertz: Long): Int = when {
        hertz <= 0L -> 0
        hertz < 20_000L -> 1
        hertz > 270_000_000_000L -> 255
        else -> (0.5 + 15.47 * ln(hertz / 18748.1)).toInt()
    }

    /** Inverse of [fcode] — the band frequency a code stands for. */
    fun fcodeToHertz(code: Int): Long = when {
        code <= 0 -> 0L
        else -> (18748.1 * exp(code / 15.47)).roundToLong()
    }
}
