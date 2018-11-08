package de.hasenburg.geofencebroker.model.storage;

import de.hasenburg.geofencebroker.main.Configuration;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Set;

/**
 * The {@link TopicAndGeofenceMapper} maps provided topics and geofences to subscription ids. Thus, it helps to identify
 * to which clients a published message should be delivered.
 */
public class TopicAndGeofenceMapper {

	private final TopicLevel anchor;

	public TopicAndGeofenceMapper(Configuration configuration) {
		anchor = new TopicLevel("ANCHOR", configuration.getGranularity());
	}

	public Set<ImmutablePair<String, Integer>> getSubscriptionIds(Topic topic, Geofence geofence) {
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

}
