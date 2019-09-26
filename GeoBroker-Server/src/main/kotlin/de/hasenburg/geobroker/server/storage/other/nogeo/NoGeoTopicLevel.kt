package de.hasenburg.geobroker.server.storage.other.nogeo

import de.hasenburg.geobroker.commons.model.message.Topic
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

private val logger = LogManager.getLogger()

const val SINGLE_LEVEL_WILDCARD: String = "+"
const val MULTI_LEVEL_WILDCARD: String = "#"

/**
 * A [NoGeoTopicLevel] is a single part of a complete [Topic]. For example, the topic a/b/c has the three
 * topic levels a, b, and c. Topic levels can also be single level wildcards [SINGLE_LEVEL_WILDCARD] or
 * multilevel wildcards [MULTI_LEVEL_WILDCARD].
 */
class NoGeoTopicLevel(val levelSpecifier: String) {

    // levelSpecifier -> TopicLevel
    private val children = ConcurrentHashMap<String, NoGeoTopicLevel>()

    // subscriptions
    private val existingSubscriptionIds = ConcurrentHashMap<String, MutableSet<ImmutablePair<String, Int>>>()
    private val numSubscriptionIds = AtomicInteger(0)
    val numberOfSubscriptionIds: Int
        get() = numSubscriptionIds.get()

    val allSubscriptionIds: Map<String, Set<ImmutablePair<String, Int>>>
        get() = existingSubscriptionIds

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    /**
     * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
     * one point none exist yet, it and all subsequent ones will be created.
     *
     * @param levelSpecifiers - the given level specifiers
     * @return the child
     */
    fun getOrCreateChild(vararg levelSpecifiers: String): NoGeoTopicLevel {
        var currentChild = this
        for (specifier in levelSpecifiers) {
            currentChild = currentChild.children.computeIfAbsent(specifier) {
                NoGeoTopicLevel(specifier)
            }
        }

        return currentChild
    }

    /**
     * Gets an already existing child for the given level specifiers. A minimum of one specifier must be provided. If at
     * one point none exist yet, this method returns null
     *
     * @param levelSpecifiers - the given level specifiers
     * @return the child or null
     */
    fun getChild(vararg levelSpecifiers: String): NoGeoTopicLevel? {
        var currentChild: NoGeoTopicLevel? = this
        for (specifier in levelSpecifiers) {
            currentChild = currentChild?.children?.get(specifier) ?: return null
        }
        return currentChild
    }

    /**
     * Stores the given [subscriptionId] in this [NoGeoTopicLevel].
     *
     * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
     *
     * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
     * @return the number of subscriptionIds stored in this [NoGeoTopicLevel] after the operation completed
     */
    fun putSubscriptionId(subscriptionId: ImmutablePair<String, Int>): Int {
        existingSubscriptionIds.getOrPut(subscriptionId.left) { ConcurrentHashMap.newKeySet() }.add(subscriptionId)
        return numSubscriptionIds.incrementAndGet()
    }

    /**
     * Removes the given [subscriptionId] in this [NoGeoTopicLevel].
     *
     * It is assumed that every subscriptionId is unique. Otherwise, inconsistencies may arise.
     *
     * @param subscriptionId - unique identifier for a subscription that comprises a clientId and an integer
     * @return the number of subscriptionIds stored in this [NoGeoTopicLevel] after the operation completed
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

    fun getDirectChild(levelSpecifier: String): NoGeoTopicLevel? {
        return children[levelSpecifier]
    }

    fun getAllDirectChildren(): Collection<NoGeoTopicLevel> {
        return children.values
    }

    /**
     * @param clientId - the id of the [Client]
     * @return Returns all subscriptionIds for the specified client.
     */
    fun getSubscriptionIdsForClientIdentifier(clientId: String): Set<ImmutablePair<String, Int>> {
        // this builds on the assumptions that this is faster then the toSet() method
        return Collections.unmodifiableSet(existingSubscriptionIds[clientId])
    }

}
