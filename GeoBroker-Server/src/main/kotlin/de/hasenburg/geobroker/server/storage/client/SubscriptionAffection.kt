package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.disgb.BrokerInfo
import de.hasenburg.geobroker.server.matching.DisGBAtPublisherMatchingLogic
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap

private val logger = LogManager.getLogger()

/**
 * This class manages what other brokers are affected by a subscription in case of [DisGBAtPublisherMatchingLogic].
 * Only the subscriptions of non remote brokers should be added here.
 *
 * Note: I know that affection != affected ;)
 */
class SubscriptionAffection {

    // client id -> subscription id + list(broker info)
    private val affections = ConcurrentHashMap<String, ConcurrentHashMap<ImmutablePair<String, Int>, List<BrokerInfo>>>()

    fun updateAffections(subscriptionId: ImmutablePair<String, Int>,
                         otherAffectedBrokers: List<BrokerInfo>): List<BrokerInfo> {

        val clientAffections = affections.getOrPut(subscriptionId.left) { ConcurrentHashMap() }

        val oldAffectedBrokers = clientAffections[subscriptionId]?.toMutableList() ?: mutableListOf()

        // brokers that are in the old but not in the new list
        val notAnymoreAffectedBrokers = oldAffectedBrokers.filter { bi -> !otherAffectedBrokers.contains(bi) }
        if (notAnymoreAffectedBrokers.isNotEmpty()) {
            logger.debug("$notAnymoreAffectedBrokers are not affected anymore by subscription $subscriptionId")
        }

        clientAffections[subscriptionId] = otherAffectedBrokers
        return notAnymoreAffectedBrokers
    }

    /**
     * @param clientIdentifier - specifies a client
     * @return all affections for the specified client
     */
    fun getAffections(clientIdentifier: String): Set<BrokerInfo> {

        val clientAffections = affections[clientIdentifier] ?: return emptySet()

        val affectionSet = mutableSetOf<BrokerInfo>()
        for (clientAffection: List<BrokerInfo> in clientAffections.values) {
            affectionSet.addAll(clientAffection)
        }

        return affectionSet
    }

    /**
     * @param subscriptionId - specifies a subscription
     * @return all affections for the specified subscription
     */
    fun getAffections(subscriptionId: ImmutablePair<String, Int>): List<BrokerInfo> {
        return this.affections[subscriptionId.left]?.get(subscriptionId) ?: emptyList()
    }

    fun removeAffections(clientIdentifier: String) {
        affections.remove(clientIdentifier)
    }

    /**
     * Returns what other brokers do not know the client yet.
     * Calculated by [updatedAffectedBrokers] - all brokers affected by any of the client's subscriptions
     * This is required as these brokers also have to receive the most up to date client location.
     */
    fun determineAffectedBrokersThatDoNotKnowTheClient(subscriptionId: ImmutablePair<String, Int>,
                                             updatedAffectedBrokers: List<BrokerInfo>): List<BrokerInfo> {
        val result = updatedAffectedBrokers.toMutableList()
        result.removeAll(getAffections(subscriptionId.left))
        return result
    }

}