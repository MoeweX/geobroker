package de.hasenburg.geobroker.commons.model.disgb

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

@Serializable
@SerialName("BrokerArea")
data class BrokerArea(val responsibleBroker: BrokerInfo, val coveredArea: Geofence) {

    fun hasResponsibleBroker(brokerId: String): Boolean {
        return responsibleBroker.brokerId == brokerId
    }

    fun containsLocation(location: Location?): Boolean {
        return coveredArea.contains(location)
    }

    fun intersects(messageGeofence: Geofence): Boolean {
        return coveredArea.intersects(messageGeofence)
    }

}

fun BrokerArea.toJson(): String {
    return Json.encodeToString(BrokerArea.serializer(), this)
}

/**
 * @throws [kotlinx.serialization.json.JsonDecodingException]
 */
fun String.toBrokerArea(): BrokerArea {
    return Json.decodeFromString(BrokerArea.serializer(), this)
}