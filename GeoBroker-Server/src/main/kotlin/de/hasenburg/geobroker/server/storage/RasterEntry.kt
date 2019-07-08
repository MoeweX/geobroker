package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.commons.lang3.tuple.ImmutablePair

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class RasterEntry(val index: Location, degreeStep: Double) {

    // TODO O: it might be faster to use Geofence.rectangle(), especially when later using the geofence
    val rasterEntryBox: Geofence = Geofence.polygon(listOf(index,
            Location(index.lat + degreeStep, index.lon),
            Location(index.lat + degreeStep, index.lon + degreeStep),
            Location(index.lat, index.lon + degreeStep))) // let's buffer this

    private val existingSubscriptionIds = ConcurrentHashMap<String, MutableSet<ImmutablePair<String, Int>>>()
    private val numSubscriptionIds = AtomicInteger(0)
    val numberOfSubscriptionIds: Int
        get() = numSubscriptionIds.get()

    /**
     * @return all subscriptions
     */
    val allSubscriptionIds: Map<String, Set<ImmutablePair<String, Int>>>
        get() = existingSubscriptionIds

    override fun toString(): String {
        return "RasterEntry{index=$index}"
    }

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    /**
     * Puts the given subscriptionId to the [RasterEntry].
     *
     * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
     *
     * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
     * @return the number of subscriptionIds stored in the [RasterEntry] after the operation completed
     */
    fun putSubscriptionId(subscriptionId: ImmutablePair<String, Int>): Int {
        existingSubscriptionIds.getOrPut(subscriptionId.left) { ConcurrentHashMap.newKeySet() }.add(subscriptionId)
        return numSubscriptionIds.incrementAndGet()
    }

    /**
     * Removes the given subscriptionId from the [RasterEntry].
     *
     * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
     *
     * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
     * @return the number of subscriptionIds stored in the [RasterEntry] after the operation completed
     */
    fun removeSubscriptionId(subscriptionId: ImmutablePair<String, Int>): Int {
        if (existingSubscriptionIds[subscriptionId.left]?.remove(subscriptionId) ?: false) {
            // if the client has entries + the id was part of the client's entries
            return numSubscriptionIds.decrementAndGet()
        } else {
            return numSubscriptionIds.get()
        }
    }

    /*****************************************************************
     * Process Published Message Operations
     ****************************************************************/

    /**
     * @param clientId - the id of the [Client]
     * @return Returns all subscriptionIds for the specified client.
     */
    fun getSubscriptionIdsForClientIdentifier(clientId: String): Set<ImmutablePair<String, Int>> {
        // this builds on the assumptions that this is faster then the toSet() method
        return Collections.unmodifiableSet(existingSubscriptionIds[clientId])
    }

}
