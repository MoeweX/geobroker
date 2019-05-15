package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import org.apache.commons.lang3.tuple.ImmutablePair


class Subscription(val subscriptionId: ImmutablePair<String, Int>, val topic: Topic, var geofence: Geofence) {

    fun getClientId(): String {
        return subscriptionId.getLeft()
    }

    override fun toString(): String {
        return "Subscription{" +
                "id=" + subscriptionId.toString() +
                "topic=" + topic +
                ", geofence=" + geofence +
                '}'
    }
}
