package de.hasenburg.geobroker.commons.model.message

import de.hasenburg.geobroker.commons.model.KryoSerializer
import de.hasenburg.geobroker.commons.model.disgb.BrokerInfo
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.GeofenceK
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.model.spatial.LocationWrapper
import kotlinx.serialization.*
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import kotlinx.serialization.protobuf.ProtoBuf
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMsg
import kotlin.system.measureTimeMillis

private val logger = LogManager.getLogger()

sealed class Payload {
    data class CONNECTPayload(val location: Location) : Payload()
    data class CONNACKPayload(val reasonCode: ReasonCode) : Payload()
    data class DISCONNECTPayload(val reasonCode: ReasonCode, val brokerInfo: BrokerInfo? = null) : Payload()
    data class PINGREQPayload(val location: Location) : Payload()
    data class PINGRESPPayload(val reasonCode: ReasonCode) : Payload()
    data class SUBSCRIBEPayload(val topic: Topic, val geofence: Geofence) : Payload()
    data class SUBACKPayload(val reasonCode: ReasonCode) : Payload()
    data class UNSUBSCRIBEPayload(val topic: Topic) : Payload()
    data class UNSUBACKPayload(val reasonCode: ReasonCode) : Payload()
    data class PUBLISHPayload(val topic: Topic, val geofence: Geofence, val content: String) : Payload()
    data class PUBACKPayload(val reasonCode: ReasonCode) : Payload()
    data class BrokerForwardDisconnectPayload(val clientIdentifier: String, val disconnectPayload: DISCONNECTPayload) :
        Payload()

    data class BrokerForwardPingreqPayload(val clientIdentifier: String, val pingreqPayload: PINGREQPayload) : Payload()
    /**
     * [publisherLocation] is needed in case of matching at the subscriber
     * [subscriberClientIdentifiers] is needed in case of matching at the publisher
     */
    data class BrokerForwardPublishPayload(val publishPayload: PUBLISHPayload,
                                           val publisherLocation: Location = Location.undefined(),
                                           val subscriberClientIdentifiers: List<String> = emptyList()) : Payload()

    data class BrokerForwardSubscribePayload(val clientIdentifier: String, val subscribePayload: SUBSCRIBEPayload) :
        Payload()

    data class BrokerForwardUnsubscribePayload(val clientIdentifier: String,
                                               val unsubscribePayload: UNSUBSCRIBEPayload) : Payload()
}

@Suppress("SpellCheckingInspection")
private enum class CPT {
    Reserved, //
    CONNECT, //
    CONNACK, //
    PUBLISH, //
    PUBACK, //
    PUBREC, //
    // PUBREL,
    // PUBCOMP,
    SUBSCRIBE, //
    SUBACK, //
    UNSUBSCRIBE, //
    UNSUBACK, //
    PINGREQ, //
    PINGRESP, //
    DISCONNECT, //
    AUTH, //

    // Inter-Broker Communication (no typical MQTT messages, so other spelling)
    BrokerForwardDisconnect, //
    BrokerForwardPingreq, //
    BrokerForwardSubscribe, //
    BrokerForwardUnsubscribe, //
    BrokerForwardPublish //
}

@Serializable
sealed class Payload2 {
    @Serializable
    @SerialName("CONNECTPayload")
    data class CONNECTPayload(val location: LocationWrapper) : Payload2()

    @Serializable
    @SerialName("CONNACKPayload")
    data class CONNACKPayload(val reasonCode: ReasonCode) : Payload2()

    @Serializable
    @SerialName("DISCONNECTPayload")
    data class DISCONNECTPayload(val reasonCode: ReasonCode, val brokerInfo: BrokerInfo? = null) : Payload2()

    @Serializable
    @SerialName("PINGREQPayload")
    data class PINGREQPayload(val location: LocationWrapper) : Payload2()

    @Serializable
    @SerialName("PINGRESPPayload")
    data class PINGRESPPayload(val reasonCode: ReasonCode, val test: String? = null) : Payload2()

    @Serializable
    @SerialName("SUBSCRIBEPayload")
    data class SUBSCRIBEPayload(val topic: Topic, val geofence: GeofenceK) : Payload2()

    @Serializable
    @SerialName("SUBACKPayload")
    data class SUBACKPayload(val reasonCode: ReasonCode) : Payload2()

    @Serializable
    @SerialName("UNSUBSCRIBEPayload")
    data class UNSUBSCRIBEPayload(val topic: Topic) : Payload2()

    @Serializable
    @SerialName("UNSUBACKPayload")
    data class UNSUBACKPayload(val reasonCode: ReasonCode) : Payload2()

    @Serializable
    @SerialName("PUBLISHPayload")
    data class PUBLISHPayload(val topic: Topic, val geofence: GeofenceK, val content: String) : Payload2()

    @Serializable
    @SerialName("PUBACKPayload")
    data class PUBACKPayload(val reasonCode: ReasonCode) : Payload2()

    @Serializable
    @SerialName("BrokerForwardDisconnectPayload")
    data class BrokerForwardDisconnectPayload(val clientIdentifier: String,
                                              val disconnectPayload: DISCONNECTPayload) : Payload2()

    @Serializable
    @SerialName("BrokerForwardPingreqPayload")
    data class BrokerForwardPingreqPayload(val clientIdentifier: String,
                                           val pingreqPayload: PINGREQPayload) : Payload2()

    /**
     * [publisherLocation] is needed in case of matching at the subscriber
     * [subscriberClientIdentifiers] is needed in case of matching at the publisher
     */
    @Serializable
    @SerialName("BrokerForwardPublishPayload")
    data class BrokerForwardPublishPayload(val publishPayload: PUBLISHPayload,
                                           val publisherLocation: LocationWrapper = LocationWrapper.Undefined,
                                           val subscriberClientIdentifiers: List<String> = emptyList()) : Payload2()

    @Serializable
    @SerialName("BrokerForwardSubscribePayload")
    data class BrokerForwardSubscribePayload(val clientIdentifier: String,
                                             val subscribePayload: SUBSCRIBEPayload) : Payload2()

    @Serializable
    @SerialName("BrokerForwardUnsubscribePayload")
    data class BrokerForwardUnsubscribePayload(val clientIdentifier: String,
                                               val unsubscribePayload: UNSUBSCRIBEPayload) : Payload2()
}

fun Payload2.toZMsg(): ZMsg? {
    return try {
        ZMsg.newStringMsg(Json(JsonConfiguration.Stable).stringify(Payload2.serializer(), this))
    } catch (e: SerializationException) {
        logger.warn("Could not create ZMsg for payload {}", this, e)
        null
    }
}

fun ZMsg.toPayload(): Payload2? {
    return try {
        Json(JsonConfiguration.Stable).parse(Payload2.serializer(), this.popString().also { destroy() })
    } catch (e: SerializationException) {
        logger.warn("Could not create Payload for received ZMsg", e)
        null
    }
}

fun ZMsg.toPayloadAndId(): Pair<String, Payload2>? {
    return try {
        val clientIdentifier = this.popString()
        val payload = this.toPayload()!!
        Pair(clientIdentifier, payload)
    } catch (e: Exception) {
        logger.error("Could not create payload and id from ZMsg", e)
        null
    }
}

/**
 * Transforms the given [Payload] to a ZMsg, these messages can be turned back to [Payload] with [ZMsg.transformZMsg].
 *
 * If the ZMsg should be send with a socket that requires an identifier, you can supply an optional [identifier]
 * parameter to this function. In this case, use [ZMsg.transformZMsgWithId] if you want to transform it back to [Payload].
 */
fun payloadToZMsg(payload: Payload, kryo: KryoSerializer, identifier: String? = null): ZMsg {
    val controlPacketType = when (payload) {
        is Payload.CONNECTPayload -> CPT.CONNECT
        is Payload.CONNACKPayload -> CPT.CONNACK
        is Payload.DISCONNECTPayload -> CPT.DISCONNECT
        is Payload.PINGREQPayload -> CPT.PINGREQ
        is Payload.PINGRESPPayload -> CPT.PINGRESP
        is Payload.SUBSCRIBEPayload -> CPT.SUBSCRIBE
        is Payload.SUBACKPayload -> CPT.SUBACK
        is Payload.UNSUBSCRIBEPayload -> CPT.UNSUBSCRIBE
        is Payload.UNSUBACKPayload -> CPT.UNSUBACK
        is Payload.PUBLISHPayload -> CPT.PUBLISH
        is Payload.PUBACKPayload -> CPT.PUBACK
        is Payload.BrokerForwardDisconnectPayload -> CPT.BrokerForwardDisconnect
        is Payload.BrokerForwardPingreqPayload -> CPT.BrokerForwardPingreq
        is Payload.BrokerForwardPublishPayload -> CPT.BrokerForwardPublish
        is Payload.BrokerForwardSubscribePayload -> CPT.BrokerForwardSubscribe
        is Payload.BrokerForwardUnsubscribePayload -> CPT.BrokerForwardUnsubscribe
    }

    return if (identifier == null) {
        ZMsg.newStringMsg(controlPacketType.name).addLast(kryo.write(payload))
    } else {
        ZMsg.newStringMsg(identifier, controlPacketType.name).addLast(kryo.write(payload))
    }
}

fun ZMsg.transformZMsg(kryo: KryoSerializer): Payload? {
    val msg = this

    try {
        if (msg.size == 2) {
            val controlPacketType = CPT.valueOf(msg.popString())
            val payload = kryo.read(msg.pop().data, controlPacketType)

            if (payload == null) {
                logger.warn("Could not create payload for $controlPacketType message, it is null")
            } else {
                return payload
            }

        } else {
            logger.error("Cannot parse message {} to MultipartMessage, has wrong size", msg.toString())
        }

    } catch (e: Exception) {
        logger.warn("Cannot parse message, due to exception, discarding it", e)
    }

    return null
}

fun ZMsg.transformZMsgWithId(kryo: KryoSerializer): Pair<String, Payload>? {
    val msg = this

    try {
        if (msg.size == 3) {
            val clientIdentifier = msg.popString()
            val controlPacketType = CPT.valueOf(msg.popString())
            val payload = kryo.read(msg.pop().data, controlPacketType)

            if (payload == null) {
                logger.warn("Could not create payload for $controlPacketType message, it is null")
            } else {
                return Pair(clientIdentifier, payload)
            }

        } else {
            logger.error("Cannot parse message {} to MultipartMessage, has wrong size", msg.toString())
        }

    } catch (e: Exception) {
        logger.warn("Cannot parse message, due to exception, discarding it", e)
    }

    return null
}

private fun KryoSerializer.read(arr: ByteArray, controlPacketType: CPT): Payload? {
    return when (controlPacketType) {
        CPT.CONNECT -> this.read(arr, Payload.CONNECTPayload::class.java)
        CPT.CONNACK -> this.read(arr, Payload.CONNACKPayload::class.java)
        CPT.DISCONNECT -> this.read(arr, Payload.DISCONNECTPayload::class.java)
        CPT.PINGREQ -> this.read(arr, Payload.PINGREQPayload::class.java)
        CPT.PINGRESP -> this.read(arr, Payload.PINGRESPPayload::class.java)
        CPT.SUBSCRIBE -> this.read(arr, Payload.SUBSCRIBEPayload::class.java)
        CPT.SUBACK -> this.read(arr, Payload.SUBACKPayload::class.java)
        CPT.UNSUBSCRIBE -> this.read(arr, Payload.UNSUBSCRIBEPayload::class.java)
        CPT.UNSUBACK -> this.read(arr, Payload.UNSUBACKPayload::class.java)
        CPT.PUBLISH -> this.read(arr, Payload.PUBLISHPayload::class.java)
        CPT.PUBACK -> this.read(arr, Payload.PUBACKPayload::class.java)
        CPT.BrokerForwardDisconnect -> this.read(arr, Payload.BrokerForwardDisconnectPayload::class.java)
        CPT.BrokerForwardPingreq -> this.read(arr, Payload.BrokerForwardPingreqPayload::class.java)
        CPT.BrokerForwardSubscribe -> this.read(arr, Payload.BrokerForwardSubscribePayload::class.java)
        CPT.BrokerForwardUnsubscribe -> this.read(arr,
                Payload.BrokerForwardUnsubscribePayload::class.java)
        CPT.BrokerForwardPublish -> this.read(arr, Payload.BrokerForwardPublishPayload::class.java)
        else -> {
            logger.error("KryoSerializer has no implementation for control packet type $controlPacketType")
            null
        }
    }
}