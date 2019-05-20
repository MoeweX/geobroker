package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.BrokerInfo
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

    fun getAffections(clientIdentifier: String): Set<BrokerInfo> {

        val clientAffections = affections[clientIdentifier] ?: return emptySet()

        val affectionSet = mutableSetOf<BrokerInfo>()
        for (clientAffection: List<BrokerInfo> in clientAffections.values) {
            affectionSet.addAll(clientAffection)
        }

        return affectionSet
    }

    fun getAffections(subscriptionId: ImmutablePair<String, Int>) : List<BrokerInfo> {
        return this.affections[subscriptionId.left]?.get(subscriptionId) ?: emptyList()
    }

    fun removeAffections(clientIdentifier: String) {
        affections.remove(clientIdentifier)
    }

}