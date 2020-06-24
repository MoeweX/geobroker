package de.hasenburg.geobroker.commons.model.message

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import org.zeromq.ZMsg

private val logger = LogManager.getLogger()

@Serializable
sealed class Payload {
    @Serializable
    @SerialName("CONNECTPayload")
    data class CONNECTPayload(val location: Location?) : Payload()

    @Serializable
    @SerialName("CONNACKPayload")
    data class CONNACKPayload(val reasonCode: ReasonCode) : Payload()

    @Serializable
    @SerialName("DISCONNECTPayload")
    data class DISCONNECTPayload(val reasonCode: ReasonCode) : Payload()

    @Serializable
    @SerialName("PINGREQPayload")
    data class PINGREQPayload(val location: Location?) : Payload()

    @Serializable
    @SerialName("PINGRESPPayload")
    data class PINGRESPPayload(val reasonCode: ReasonCode, val test: String? = null) : Payload()

    @Serializable
    @SerialName("SUBSCRIBEPayload")
    data class SUBSCRIBEPayload(val topic: Topic, val geofence: Geofence) : Payload()

    @Serializable
    @SerialName("SUBACKPayload")
    data class SUBACKPayload(val reasonCode: ReasonCode) : Payload()

    @Serializable
    @SerialName("UNSUBSCRIBEPayload")
    data class UNSUBSCRIBEPayload(val topic: Topic) : Payload()

    @Serializable
    @SerialName("UNSUBACKPayload")
    data class UNSUBACKPayload(val reasonCode: ReasonCode) : Payload()

    @Serializable
    @SerialName("PUBLISHPayload")
    data class PUBLISHPayload(val topic: Topic, val geofence: Geofence, val content: String) : Payload()

    @Serializable
    @SerialName("PUBACKPayload")
    data class PUBACKPayload(val reasonCode: ReasonCode) : Payload()

}

fun Payload.toZMsg(json: Json = Json(JsonConfiguration.Stable), clientIdentifier: String? = null): ZMsg {
    return if (clientIdentifier != null) {
        ZMsg.newStringMsg(clientIdentifier, json.stringify(Payload.serializer(), this))
    } else {
        ZMsg.newStringMsg(json.stringify(Payload.serializer(), this))
    }
}

fun ZMsg.toPayload(json: Json = Json(JsonConfiguration.Stable)): Payload? {
    return try {
        json.parse(Payload.serializer(), this.popString().also { destroy() })
    } catch (e: SerializationException) {
        logger.warn("Could not create Payload for received ZMsg", e)
        null
    }
}

fun ZMsg.toPayloadAndId(json: Json = Json(JsonConfiguration.Stable)): Pair<String, Payload>? {
    return try {
        val clientIdentifier = this.popString()
        val payload = this.toPayload(json)!!
        Pair(clientIdentifier, payload)
    } catch (e: Exception) {
        logger.error("Could not create payload and id from ZMsg", e)
        null
    }
}
