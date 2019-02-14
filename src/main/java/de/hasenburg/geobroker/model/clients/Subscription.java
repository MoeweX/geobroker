package de.hasenburg.geobroker.model.clients;

import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.spatial.Geofence;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class Subscription {

	private final ImmutablePair<String, Integer> subscriptionId; // clientId -> unique identifier

	private final Topic topic;
	private Geofence geofence;

	public Subscription(ImmutablePair<String, Integer> subscriptionId, Topic topic, Geofence geofence) {
		this.topic = topic;
		this.geofence = geofence;
		this.subscriptionId = subscriptionId;
	}

	protected ImmutablePair<String, Integer> getSubscriptionId() {
		return this.subscriptionId;
	}

	protected Topic getTopic() {
		return topic;
	}

	protected Geofence getGeofence() {
		return geofence;
	}

	protected void setGeofence(Geofence geofence) {
		this.geofence = geofence;
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
