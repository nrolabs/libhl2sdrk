# libhl2sdrk

Kotlin driver for the **Hermes-Lite 2** amateur-radio SDR transceiver, speaking
the openHPSDR **Protocol 1** (Metis) over UDP. Pure Kotlin — no NDK, no native
libraries, no root. Built for Android but the wire codec is Android-free and
JVM-testable.

Maintained by Isak — **PU3IAR**. Brought to you by [id.qsl.br](https://id.qsl.br),
a platform with tools for amateur radio operators.
YouTube: [@qraisak](https://www.youtube.com/@qraisak)

## Features

- **`Hl2Protocol`** — the openHPSDR Protocol 1 frame codec, free of any Android
  dependency and unit-tested on the JVM:
  - UDP **discovery** and start/stop.
  - **RX** frame parsing: 24-bit interleaved IQ → `FloatArray` (`i0,q0,…` in
    `[-1,1]`), for **1 or 2 receivers** (variable slot layout).
  - Full **C0 command-and-control** bank map: RX1/RX2 NCO frequency, sample
    rate (48/96/192/384 kHz), number of receivers, LNA gain, duplex, TX NCO
    frequency, TX drive, PA enable, **MOX/PTT**.
  - **TX** sample streaming (48 kSps interleaved IQ) and **telemetry** decode
    (PA temperature, forward/reverse power, PA current).
- **`Hl2Client`** — UDP transport and streaming lifecycle:
  - LAN discovery (or a fixed board IP), RX accumulation into display blocks and
    a power spectrum, delivered through the same callback contract as the
    RTL-SDR clients so a host app can drive either device.
  - A dedicated TX sender that emits a transmit frame only once a full frame of
    audio is queued, **locking the downstream to 48 kSps** (the mic clock)
    regardless of the RX frame rate or receiver count.
  - `setFrequency` / `setFrequency2` / `setActiveReceiver` / `setSampleRate` /
    `setLnaGain` / `setReceiverCount`, and TX: `setTxFrequency` / `setTxDrive` /
    `setPaEnabled` / `setPtt` / `submitTxIq`.

## Testing without hardware

The companion **HL2 protocol emulator** in the parent project
(the `hl2-emulator` in the project tools)
speaks Protocol 1 back to a client: discovery, synthetic RX signals (with a
noise floor), telemetry, and a TX audio monitor — so the driver can be
developed and validated with no board attached.

## Credits and references

Faithful port of the reference host driver and the openHPSDR / Hermes-Lite 2
documentation:

- **Hermes-Lite 2** project (hardware, gateware, register docs) — Steve Haynal
  (KF7O) and contributors: <https://github.com/softerhardware/Hermes-Lite2>
- Reference driver `hl2.cxx` from **Quisk** — James Ahlstrom (N2ADR):
  <https://github.com/softerhardware/Hermes-Lite2/blob/master/software/hl2setup/hl2.cxx>
- **openHPSDR Protocol 1** (Metis/USB) specification, openHPSDR project.

I2C addresses and register semantics follow the C reference and the HL2 wiki.

## License

GNU General Public License **v2 or later** — see [`LICENSE`](LICENSE).
Kotlin port © 2026 Isak Ruas <isakruas@gmail.com>.
