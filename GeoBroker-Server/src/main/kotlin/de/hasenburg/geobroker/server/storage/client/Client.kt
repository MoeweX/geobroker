package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.matching.DisGBAtPublisherMatchingLogic
import org.apache.commons.lang3.tuple.ImmutablePair
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * [clientIdentifier] must be unique
 * [location] might be null, if none has been supplied yet
 * [remote] indicates whether the client is connected to another broker, so far only needed for [DisGBAtPublisherMatchingLogic]
 */
class Client(val clientIdentifier: String, location: Location?, val remote: Boolean = false) {

    var location: Location? = location
        private set // prevent to be set by other classes

    var heartbeat: Long = 0
    private val lastSubscriptionId = AtomicInteger(0)
    private val subscriptions = ConcurrentHashMap<Topic, Subscription>()

    val subscriptionCount: Int
        get() = subscriptions.keys.size

    init {
        updateHeartbeat()
    }

    /*****************************************************************
     * Locations
     ****************************************************************/

    fun updateLocation(location: Location?) {
        updateHeartbeat()
        this.location = location
    }

    fun createAndPutSubscription(topic: Topic, geofence: Geofence): ImmutablePair<String, String> {
        updateHeartbeat()
        val s = Subscription(ImmutablePair(clientIdentifier, lastSubscriptionId.incrementAndGet().toString()), topic, geofence)
        subscriptions[topic] = s
        return s.subscriptionId
    }

    fun getSubscription(topic: Topic): Subscription? {
        return subscriptions[topic]
    }

    fun removeSubscription(topic: Topic): Subscription? {
        val s = subscriptions.remove(topic) ?: return null
        return s
    }

    /*****************************************************************
     * Others
     ****************************************************************/

    private fun updateHeartbeat() {
        heartbeat = System.currentTimeMillis()
    }

    override fun toString(): String {
        val s = StringBuilder("\n")
        for (subscription in subscriptions.values) {
            s.append("$subscription, ")
        }

        return "Client{heartbeat=$heartbeat, clientIdentifier=$clientIdentifier, location=$location, subscriptions=$s}"
    }

}
