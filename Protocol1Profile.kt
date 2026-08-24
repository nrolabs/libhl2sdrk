package com.isaklab.libhl2sdrk

import android.util.Log
import com.isaklab.isdrproto.DriverProto
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketTimeoutException

/**
 * Metis/openHPSDR Protocol 1 discovery product ids.
 *
 * These values deliberately live in a P1-only namespace.  Protocol 2 reuses
 * some integers for different products (notably 10 = Saturn there, while it
 * is Orion MkII here), so sharing a generic board-id enum would make a valid
 * packet easy to authenticate as the wrong hardware family.
 */
object Protocol1DiscoveryBoardId {
    const val HERMES = 1
    const val HERMES_II = 2
    const val ANGELIA = 4
    const val ORION = 5
    const val HERMES_LITE = 6
    const val ORION_MK_II = 10
}

/**
 * Exact product profile selected by the operator and the discovery board id
 * that is allowed to satisfy it.
 *
 * Several ANAN chassis share one FPGA board family (10/100 = Hermes,
 * 10E/100B = Hermes II, 7000/8000 = Orion MkII). Discovery can authenticate
 * the board family; the exact chassis remains the explicit product choice
 * carried by CMD_OPEN and controls PA/rating/capabilities above this codec.
 */
enum class Protocol1Profile(
    val openFlags: Int,
    val discoveryBoardId: Int,
    val receiverCapacity: Int,
    val physicalAdcCount: Int,
    val displayName: String,
) {
    HERMES_LITE_2(0, Protocol1DiscoveryBoardId.HERMES_LITE, 4, 1, "Hermes-Lite 2"),
    ANAN_10(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN10), Protocol1DiscoveryBoardId.HERMES, 4, 1, "ANAN-10"),
    ANAN_100(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN100), Protocol1DiscoveryBoardId.HERMES, 4, 1, "ANAN-100"),
    ANAN_10E(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN10E), Protocol1DiscoveryBoardId.HERMES_II, 2, 1, "ANAN-10E"),
    ANAN_100B(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN100B), Protocol1DiscoveryBoardId.HERMES_II, 2, 1, "ANAN-100B"),
    ANAN_100D(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN100D), Protocol1DiscoveryBoardId.ANGELIA, 4, 2, "ANAN-100D"),
    ANAN_200D(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN200D), Protocol1DiscoveryBoardId.ORION, 4, 2, "ANAN-200D"),
    ANAN_7000DLE(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN7000), Protocol1DiscoveryBoardId.ORION_MK_II, 4, 2, "ANAN-7000DLE"),
    ANAN_8000DLE(DriverProto.hpsdrClassicOpenFlags(DriverProto.OPEN_HPSDR_CHASSIS_ANAN8000), Protocol1DiscoveryBoardId.ORION_MK_II, 4, 2, "ANAN-8000DLE");

    val usesClassicCodec: Boolean get() = this != HERMES_LITE_2

    /** Only HL2 feedback routing is proved end to end in the Android codec. */
    val pureSignalSupported: Boolean get() = this == HERMES_LITE_2

    /** Proven coherent diversity, never inferred merely from DDC count. */
    val diversitySupported: Boolean get() = usesClassicCodec && physicalAdcCount >= 2

    fun acceptsDiscoveryReply(bytes: ByteArray, length: Int): Boolean =
        Hl2Protocol.isDiscoveryReply(bytes, length) &&
            Hl2Protocol.boardIdOf(bytes) == discoveryBoardId

    companion object {
        /** Null means legacy/unknown flags and must be refused before hardware mutation. */
        fun fromOpenFlags(flags: Int): Protocol1Profile? =
            entries.singleOrNull { it.openFlags == flags }
    }
}

/** Result token proving discovery matched [profile] before any control frame is emitted. */
class VerifiedProtocol1Board internal constructor(
    val address: InetAddress,
    val profile: Protocol1Profile,
)

/** Read-only Metis discovery preflight shared by DriverSession and Hl2Client. */
object Protocol1Discovery {
    private const val TAG = "Protocol1Discovery"

    fun find(profile: Protocol1Profile, host: String, port: Int): VerifiedProtocol1Board? {
        val target = InetAddress.getByName(host)
        val broadcast = target.hostAddress == Hl2Client.BROADCAST
        val socket = DatagramSocket()
        try {
            socket.broadcast = true
            socket.soTimeout = 500
            val request = Hl2Protocol.discoveryRequest()
            val reply = ByteArray(64)
            repeat(4) {
                socket.send(DatagramPacket(request, request.size, target, port))
                while (true) {
                    try {
                        val packet = DatagramPacket(reply, reply.size)
                        socket.receive(packet)
                        if (!broadcast && packet.address != target) continue
                        if (!Hl2Protocol.isDiscoveryReply(reply, packet.length)) continue
                        val boardId = Hl2Protocol.boardIdOf(reply)
                        if (profile.acceptsDiscoveryReply(reply, packet.length)) {
                            Log.i(
                                TAG,
                                "verified ${profile.displayName} board_id=0x%02x at %s"
                                    .format(boardId, packet.address.hostAddress),
                            )
                            return VerifiedProtocol1Board(packet.address, profile)
                        }
                        Log.w(
                            TAG,
                            "ignoring board_id=0x%02x at %s; ${profile.displayName} requires 0x%02x"
                                .format(
                                    boardId,
                                    packet.address.hostAddress,
                                    profile.discoveryBoardId,
                                ),
                        )
                    } catch (_: SocketTimeoutException) {
                        break
                    }
                }
            }
        } finally {
            socket.close()
        }
        return null
    }
}
