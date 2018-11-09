package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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

	public Set<ImmutablePair<String, Integer>> getSubscriptionIds(Topic topic, Location publisherLocation) {
		// get TopicLevel that match Topic
		List<TopicLevel> matchingTopicLevels = getMatchingTopicLevels(topic);

		// get subscription ids from raster for publisher location
		// TODO
		return null;
	}

	public void putSubscriptionId(Set<ImmutablePair<String, Integer>> subscriptionId, Topic topic, Geofence geofence) {
		// TODO
	}

	public void removeSubscriptionId(Set<ImmutablePair<String, Integer>> subscriptionId, Topic topic,
									 Geofence geofence) {
		// TODO
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
					if (levelSpecifier.equals(child.getLevelSpecifier()) || TopicLevel.SINGLE_LEVEL_WILDCARD.equals(
							child.getLevelSpecifier())) {
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

		// add multilevel wildcards to all others and return
		return Stream.concat(currentLevelsInWhichChildrenHaveToBeChecked.stream(),
							 multiLevelWildcardTopicLevels.stream()).collect(Collectors.toList());
	}

	/*****************************************************************
	 * Generated methods
	 ****************************************************************/

	protected TopicLevel getAnchor() {
		return anchor;
	}
}
