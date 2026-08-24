package com.isaklab.libhl2sdrk

/**
 * Physical ADC input consumed by one classic Protocol-1 DDC.
 *
 * The supported exact profiles prove at most two ADCs. The two-bit wire field
 * reserves more values, but exposing an unproved input would be a capability
 * claim, so the public type deliberately contains only ADC1 and ADC2.
 */
enum class RxAdc(val protocol1Code: Int) {
    ADC1(0),
    ADC2(1),
}

/**
 * Phase-coherent topology the classic Protocol-1 wire can actually express.
 * Bit 7 of C0=0 synchronises DDC0/DDC1 specifically; it is not an arbitrary
 * receiver mask.
 */
enum class DiversityMode {
    DISABLED,
    RX1_RX2,
}
