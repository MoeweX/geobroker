package de.hasenburg.geobroker.server.storage

import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.server.main.Configuration
import org.apache.commons.lang3.tuple.ImmutablePair

import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * The [TopicAndGeofenceMapper] maps provided topics and geofences to subscription ids. Thus, it helps to identify
 * to which clients a published message should be delivered.
 */
class TopicAndGeofenceMapper(configuration: Configuration) {

    val anchor: TopicLevel = TopicLevel("ANCHOR", configuration.granularity)

    /*****************************************************************
     * Subscribe/Unsubscribe Operations
     ****************************************************************/

    fun putSubscriptionId(subscriptionId: ImmutablePair<String, Int>, topic: Topic, geofence: Geofence) {
        val level = anchor.getOrCreateChildren(*topic.levelSpecifiers)
        level.raster.putSubscriptionIdIntoRasterEntries(geofence, subscriptionId)
    }

    fun removeSubscriptionId(subscriptionId: ImmutablePair<String, Int>, topic: Topic, geofence: Geofence) {
        val level = anchor.getChildren(*topic.levelSpecifiers) ?: return
        level.raster.removeSubscriptionIdFromRasterEntries(geofence, subscriptionId)
    }

    /*****************************************************************
     * Process Published Message Operations
     ****************************************************************/

    /**
     * Gets all subscription ids for clients that subscribed to the given [Topic] and that have subscribed to a
     * [Geofence] which contains the publisher's current [Location].
     *
     * @param topic - see above
     * @param publisherLocation - see above
     * @return see above
     */
    fun getSubscriptionIds(topic: Topic, publisherLocation: Location): Set<ImmutablePair<String, Int>> {
        // get TopicLevel that match Topic
        val matchingTopicLevels = getMatchingTopicLevels(topic)

        // get subscription ids from raster for publisher location
        val subscriptionIds = HashSet<ImmutablePair<String, Int>>()
        for (matchingTopicLevel in matchingTopicLevels) {
            val tmp = matchingTopicLevel.raster.getSubscriptionIdsForPublisherLocation(publisherLocation)
            for (set in tmp.values) {
                subscriptionIds.addAll(set)
            }
        }

        return subscriptionIds
    }

    /**
     * Gets all [TopicLevel] that match the given topic. The given topic may not have any wildcards, as this
     * method is used to get all subscribers for a published message.
     *
     * @param topic - the topic used; it may not have any wildcards
     * @return the matching [TopicLevel]
     */
    fun getMatchingTopicLevels(topic: Topic): List<TopicLevel> {
        var currentLevelsInWhichChildrenHaveToBeChecked: MutableList<TopicLevel> = mutableListOf()
        var nextLevelsInWhichChildrenHaveToBeChecked: MutableList<TopicLevel> = mutableListOf()
        val multiLevelWildcardTopicLevels = mutableListOf<TopicLevel>()

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
