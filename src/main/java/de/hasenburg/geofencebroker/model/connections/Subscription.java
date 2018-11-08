package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class Subscription {

	private final ImmutablePair<String, Integer> subscriptionId; // clientId -> unique identifier

	private Topic topic;
	private Geofence geofence;

	public Subscription(ImmutablePair<String, Integer> subscriptionId, Topic topic, Geofence geofence) {
		this.topic = topic;
		this.geofence = geofence;
		this.subscriptionId = subscriptionId;
	}

	public Topic getTopic() {
		return topic;
	}

	public Geofence getGeofence() {
		return geofence;
	}

	@Override
	public String toString() {
		return "Subscription{" +
				"id=" + subscriptionId.toString() +
				"topic=" + topic +
				", geofence=" + geofence +
				'}';
	}
}
