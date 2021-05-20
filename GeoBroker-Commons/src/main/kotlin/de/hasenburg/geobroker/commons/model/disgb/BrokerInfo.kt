package de.hasenburg.geobroker.commons.model.disgb

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

/**
 * Stores information of a given broker.
 *
 * [brokerId] - unique id of the broker
 * [ip] - this is the public ip used by other brokers, so should be reachable via the internet
 * [port] - server port.
 */
@Serializable
data class BrokerInfo(val brokerId: String, val ip: String, val port: Int)

fun BrokerInfo.toJson() : String {
    return Json.encodeToString(BrokerInfo.serializer(), this)
}

/**
 * @throws [kotlinx.serialization.json.JsonDecodingException]
 */
fun String.toBrokerInfo() : BrokerInfo {
    return Json.decodeFromString(BrokerInfo.serializer(), this)
}