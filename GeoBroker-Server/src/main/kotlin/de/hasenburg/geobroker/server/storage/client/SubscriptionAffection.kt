package de.hasenburg.geobroker.server.storage.client

import de.hasenburg.geobroker.commons.model.BrokerInfo
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap
import de.hasenburg.geobroker.server.matching.DisGBAtPublisherMatchingLogic

private val logger = LogManager.getLogger()

/**
 * This class manages what other brokers are affected by a subscription in case of [DisGBAtPublisherMatchingLogic].
 * Only the subscriptions of non remote brokers should be added here.
 *
 * Note: I know that affection != affected ;)
 */
class SubscriptionAffection {

    val affections = ConcurrentHashMap<ImmutablePair<String, Int>, List<BrokerInfo>>()

    fun updateAffections(subscriptionId: ImmutablePair<String, Int>,
                         otherAffectedBrokers: List<BrokerInfo>): List<BrokerInfo> {

        val oldAffectedBrokers = affections[subscriptionId]?.toMutableList() ?: mutableListOf()

        // brokers that are in the old but not in the new list
        val notAnymoreAffectedBrokers = oldAffectedBrokers.filter { bi -> !otherAffectedBrokers.contains(bi) }
        if (notAnymoreAffectedBrokers.size > 0) {
            logger.debug("$oldAffectedBrokers are not affected anymore by subscription $subscriptionId")
        }

        affections.put(subscriptionId, otherAffectedBrokers)
        return notAnymoreAffectedBrokers
    }

}