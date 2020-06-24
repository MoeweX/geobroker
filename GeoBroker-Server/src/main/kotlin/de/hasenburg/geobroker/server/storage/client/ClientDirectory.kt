package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap

private val logger = LogManager.getLogger()

class ClientDirectory {

    private val clients = ConcurrentHashMap<String, Client>()

    val numberOfClients: Int
        get() = clients.size

    /*****************************************************************
     * Clients
     ****************************************************************/

    fun clientExists(clientIdentifier: String): Boolean {
        return clients.containsKey(clientIdentifier)
    }

    /**
     * Add a client to the directory, if it did not exist before.
     *
     * @param clientIdentifier of the to be added client
     * @param initialLocation - the initial location of the client
     * @param remote - whether the client is a remote client (connected to another broker)
     * @return true, if added
     */
    fun addClient(clientIdentifier: String, initialLocation: Location?): Boolean {
        logger.trace("Connecting client {}, is remote: {}", clientIdentifier)
        if (clients.containsKey(clientIdentifier)) {
            logger.warn("Tried to add client {}, but already existed", clientIdentifier)
            return false
        }
        clients[clientIdentifier] = Client(clientIdentifier, initialLocation)
        return true
    }

    fun getClient(clientIdentifier: String): Client? {
        return clients[clientIdentifier]
    }

    /**
     * Removes a client from the directory.
     * TODO B: the client's subscriptions should be removed from the TopicAndGeofenceMapper as well
     *
     * @param clientIdentifier of the to be removed client
     * @return true, if client existed before
     */
    fun removeClient(clientIdentifier: String): Boolean {
        logger.trace("Removing client {}", clientIdentifier)
        if (clients.remove(clientIdentifier) == null) {
            logger.warn("Tried to remove client, but did not exist")
            return false
        }
        return true
    }

    /**
     * Returns true if the location was updated and false if the [Client] with the given [clientIdentifier] did not
     * exist.
     */
    fun updateClientLocation(clientIdentifier: String, location: Location?): Boolean {
        logger.trace("Updating client {} location to {}", clientIdentifier, location)
        val c = clients[clientIdentifier] ?: return false

        c.updateLocation(location)
        return true
    }

    /**
     * Returns the client's [Location] or null, if the client did not exist or it does not have a location.
     */
    fun getClientLocation(clientIdentifier: String): Location? {
        logger.trace("Retrieving location of client {}", clientIdentifier)
        val c = clients[clientIdentifier] ?: return null

        return c.location

    }

    /*****************************************************************
     * Subscriptions of Clients
     ****************************************************************/

    fun getCurrentClientSubscriptions(clientIdentifier: String): Int {
        logger.trace("Checking amount of active subscriptions for client {}", clientIdentifier)
        val c = clients[clientIdentifier] ?: return 0

        return c.subscriptionCount
    }

    fun getSubscription(clientIdentifier: String, topic: Topic): Subscription? {
        return clients[clientIdentifier]?.getSubscription(topic)
    }

    /**
     * Checks whether a [Client] is subscribed to the given [Topic]. If so, it calculates the [Geofence]
     * difference (old geofence - given geofence) and returns it together with the subscription identifier of the
     * existing [Subscription]. If not subscribed yet, return null
     *
     * Reasoning is that returned geofence can be used to unsubscribe from now unrelated parts.
     *
     * !!!!!!!!!!!!!!!!!!!!!
     *
     * NOTE: For now, the method returns the existing subscription's original geofence and does not calculate the
     * difference
     *
     * !!!!!!!!!!!!!!!!!!!!!
     *
     * @param clientIdentifier - the identifier of the [Client]
     * @param topic - see above
     * @param geofence - see above
     * @return see above
     */
    fun checkIfSubscribed(clientIdentifier: String, topic: Topic,
                          geofence: Geofence): ImmutablePair<ImmutablePair<String, Int>, Geofence>? {
        val c = clients[clientIdentifier] ?: return null
        val s = c.getSubscription(topic) ?: return null
        return ImmutablePair(s.subscriptionId, s.geofence)
    }

    /**
     * Creates a new [Subscription] based on the provided information. Returns the subscription id of the
     * subscription. In case one existed already, only the geofence is updated.
     *
     * @param clientIdentifier - identifier of the [Client]
     * @param topic - topic of subscription
     * @param geofence - geofence of subscription
     * @return the above specified subscription id or null if no client existed
     */
    fun updateSubscription(clientIdentifier: String, topic: Topic, geofence: Geofence): ImmutablePair<String, Int>? {
        val c = clients[clientIdentifier] ?: return null

        val s = c.getSubscription(topic) ?: return c.createAndPutSubscription(topic, geofence)

        // a subscription existed, so we need to update the geofence to the new one
        s.geofence = geofence

        return s.subscriptionId
    }

    /**
     * Removes an existing [Subscription] from a [Client], if it exists.
     *
     * @param clientIdentifier - identifier of the [Client]
     * @param topic - topic of to be removed subscription
     * @return the removed [Subscription] or null, if none existed
     */
    fun removeSubscription(clientIdentifier: String, topic: Topic): Subscription? {
        val c = clients[clientIdentifier] ?: return null

        return c.removeSubscription(topic)
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    override fun toString(): String {
        val s = StringBuilder()
        for ((_, value) in clients) {
            s.append(value.toString())
        }
        return s.toString()
    }

}
