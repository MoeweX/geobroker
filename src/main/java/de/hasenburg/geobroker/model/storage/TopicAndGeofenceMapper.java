package de.hasenburg.geobroker.model.storage;

import de.hasenburg.geobroker.main.Configuration;
import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.spatial.Geofence;
import de.hasenburg.geobroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link TopicAndGeofenceMapper} maps provided topics and geofences to subscription ids. Thus, it helps to identify
 * to which clients a published message should be delivered.
 */
public class TopicAndGeofenceMapper {

	private final TopicLevel anchor;

	public TopicAndGeofenceMapper(Configuration configuration) {
		anchor = new TopicLevel("ANCHOR", configuration.getGranularity());
	}

	/*****************************************************************
	 * Subscribe/Unsubscribe Operations
	 ****************************************************************/

	public void putSubscriptionId(ImmutablePair<String, Integer> subscriptionId, Topic topic, Geofence geofence) {
		TopicLevel level = anchor.getOrCreateChildren(topic.getLevelSpecifiers());
		level.getRaster().putSubscriptionIdIntoRasterEntries(geofence, subscriptionId);
	}

	public void removeSubscriptionId(ImmutablePair<String, Integer> subscriptionId, Topic topic, Geofence geofence) {
		TopicLevel level = anchor.getChildren(topic.getLevelSpecifiers());
		if (level == null) {
			return;
		}
		level.getRaster().removeSubscriptionIdFromRasterEntries(geofence, subscriptionId);
	}

	/*****************************************************************
	 * Process Published Message Operations
	 ****************************************************************/

	/**
	 * Gets all subscription ids for clients that subscribed to the given {@link Topic} and that have subscribed to a
	 * {@link Geofence} which contains the publisher's current {@link Location}.
	 *
	 * @param topic - see above
	 * @param publisherLocation - see above
	 * @return see above
	 */
	public Set<ImmutablePair<String, Integer>> getSubscriptionIds(Topic topic, Location publisherLocation) {
		// get TopicLevel that match Topic
		List<TopicLevel> matchingTopicLevels = getMatchingTopicLevels(topic);

		// get subscription ids from raster for publisher location
		HashSet<ImmutablePair<String, Integer>> subscriptionIds = new HashSet<>();
		for (TopicLevel matchingTopicLevel : matchingTopicLevels) {
			Map<String, Set<ImmutablePair<String, Integer>>> tmp =
					matchingTopicLevel.getRaster().getSubscriptionIdsForPublisherLocation(publisherLocation);
			for (Set<ImmutablePair<String, Integer>> set : tmp.values()) {
				subscriptionIds.addAll(set);
			}
		}

		return subscriptionIds;
	}

	/**
	 * Gets all {@link TopicLevel} that match the given topic. The given topic may not have any wildcards, as this
	 * method is used to get all subscribers for a published message.
	 *
	 * @param topic - the topic used; it may not have any wildcards
	 * @return the matching {@link TopicLevel}
	 */
	protected List<TopicLevel> getMatchingTopicLevels(Topic topic) {
		List<TopicLevel> currentLevelsInWhichChildrenHaveToBeChecked = new ArrayList<>();
		List<TopicLevel> nextLevelsInWhichChildrenHaveToBeChecked = new ArrayList<>();
		List<TopicLevel> multiLevelWildcardTopicLevels = new ArrayList<>();

		// add anchor
		currentLevelsInWhichChildrenHaveToBeChecked.add(anchor);

		// traverse the TopicLevel tree
		for (int levelIndex = 0; levelIndex < topic.getNumberOfLevels(); levelIndex++) {
			String levelSpecifier = topic.getLevelSpecifier(levelIndex);
			// look into each to be checked topic level
			for (TopicLevel topicLevel : currentLevelsInWhichChildrenHaveToBeChecked) {
				// for each children, check whether important
				Collection<TopicLevel> children = topicLevel.getAllDirectChildren();
				for (TopicLevel child : children) {

					if (levelSpecifier.equals(child.getLevelSpecifier()) ||
							TopicLevel.SINGLE_LEVEL_WILDCARD.equals(child.getLevelSpecifier())) {

						// important for single level
						nextLevelsInWhichChildrenHaveToBeChecked.add(child);

					} else if (TopicLevel.MULTI_LEVEL_WILDCARD.equals(child.getLevelSpecifier())) {

						// important for multi level
						multiLevelWildcardTopicLevels.add(child);

					}
					
				}
			}

			// update currentLevelsInWhichChildrenHaveToBeChecked
			currentLevelsInWhichChildrenHaveToBeChecked = nextLevelsInWhichChildrenHaveToBeChecked;
			nextLevelsInWhichChildrenHaveToBeChecked = new ArrayList<>();
		}

		// check if there are any multi-level wildcards left in the next level -> if so add to multi level list
		for (TopicLevel topicLevel : currentLevelsInWhichChildrenHaveToBeChecked) {
			Collection<TopicLevel> children = topicLevel.getAllDirectChildren();
			for (TopicLevel child : children) {
				if (TopicLevel.MULTI_LEVEL_WILDCARD.equals(child.getLevelSpecifier())) {
					multiLevelWildcardTopicLevels.add(child);
				}
			}
		}


		// add multilevel wildcards to all others and return
		return Stream
				.concat(currentLevelsInWhichChildrenHaveToBeChecked.stream(), multiLevelWildcardTopicLevels.stream())
				.collect(Collectors.toList());
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	protected TopicLevel getAnchor() {
		return anchor;
	}
}
