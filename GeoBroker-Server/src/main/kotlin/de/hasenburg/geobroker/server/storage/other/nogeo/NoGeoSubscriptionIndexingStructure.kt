package de.hasenburg.geobroker.server.storage.other.nogeo

import de.hasenburg.geobroker.commons.model.message.Topic
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.logging.log4j.LogManager

import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

private val logger = LogManager.getLogger()

/**
 * The [NoGeoSubscriptionIndexingStructure] maps provided topics to subscription ids. Thus, it helps to identify
 * to which clients a published message should be delivered.
 *
 * The implementation of the NoGeoStorage uses TopicMatching only; i.e., it only uses topics to identify matching
 * subscriptions.
 */
class NoGeoSubscriptionIndexingStructure() {

    val anchor: NoGeoTopicLevel = NoGeoTopicLevel("ANCHOR")

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    fun putSubscriptionId(subscriptionId: ImmutablePair<String, Int>, topic: Topic) {
        val level = anchor.getOrCreateChild(*topic.levelSpecifiers)
        level.putSubscriptionId(subscriptionId)
    }

    fun removeSubscriptionId(subscriptionId: ImmutablePair<String, Int>, topic: Topic) {
        val level = anchor.getChild(*topic.levelSpecifiers) ?: return
        level.removeSubscriptionId(subscriptionId)
    }

    /*****************************************************************
     * Process Published Message Operations
     ****************************************************************/

    /**
     * Gets all subscription ids for clients that subscribed to the given [Topic].
     *
     * @param topic - see above
     * @return see above
     */
    fun getSubscriptionIds(topic: Topic): List<ImmutablePair<String, Int>> {

        // get TopicLevel that match Topic
        val matchingTopicLevels = getMatchingTopicLevels(topic)

        // get subscription ids from raster for publisher location
        val subscriptionIds = mutableListOf<ImmutablePair<String, Int>>()
        for (matchingTopicLevel in matchingTopicLevels) {
            val clientSets = matchingTopicLevel.allSubscriptionIds.values;
            for (clientSet in clientSets) {
                subscriptionIds.addAll(clientSet)
            }
        }

        return subscriptionIds
    }

    /**
     * Gets all [NoGeoTopicLevel] that match the given topic. The given topic may not have any wildcards, as this
     * method is used to get all subscribers for a published message.
     *
     * @param topic - the topic used; it may not have any wildcards
     * @return the matching [NoGeoTopicLevel]
     */
    fun getMatchingTopicLevels(topic: Topic): List<NoGeoTopicLevel> {
        var currentLevelsInWhichChildrenHaveToBeChecked: MutableList<NoGeoTopicLevel> = mutableListOf()
        var nextLevelsInWhichChildrenHaveToBeChecked: MutableList<NoGeoTopicLevel> = mutableListOf()
        val multiLevelWildcardTopicLevels = mutableListOf<NoGeoTopicLevel>()

        // add anchor
        currentLevelsInWhichChildrenHaveToBeChecked.add(anchor)

        // traverse the TopicLevel tree
        for (levelIndex in 0 until topic.numberOfLevels) {
            val levelSpecifier = topic.getLevelSpecifier(levelIndex)
            // look into each to be checked topic level
            for (topicLevel in currentLevelsInWhichChildrenHaveToBeChecked) {
                // for each children, check whether important
                val children = topicLevel.getAllDirectChildren()
                for (child in children) {
                    if (levelSpecifier == child.levelSpecifier || SINGLE_LEVEL_WILDCARD == child.levelSpecifier) {
                        // important for single level
                        nextLevelsInWhichChildrenHaveToBeChecked.add(child)
                    } else if (MULTI_LEVEL_WILDCARD == child.levelSpecifier) {
                        // important for multi level
                        multiLevelWildcardTopicLevels.add(child)
                    }
                }
            }

            // update currentLevelsInWhichChildrenHaveToBeChecked
            currentLevelsInWhichChildrenHaveToBeChecked = nextLevelsInWhichChildrenHaveToBeChecked
            nextLevelsInWhichChildrenHaveToBeChecked = ArrayList()
        }

        // check if there are any multi-level wildcards left in the next level -> if so add to multi level list
        for (topicLevel in currentLevelsInWhichChildrenHaveToBeChecked) {
            val children = topicLevel.getAllDirectChildren()
            for (child in children) {
                if (MULTI_LEVEL_WILDCARD == child.levelSpecifier) {
                    multiLevelWildcardTopicLevels.add(child)
                }
            }
        }

        // add multilevel wildcards to all others and return
        return Stream.concat(currentLevelsInWhichChildrenHaveToBeChecked.stream(),
                multiLevelWildcardTopicLevels.stream()).collect(Collectors.toList())
    }
}
