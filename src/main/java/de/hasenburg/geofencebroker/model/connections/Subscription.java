package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Topic;

public class Subscription {

	private Topic topic;
	private String geofence;

	public Subscription(Topic topic, String geofence) {
		this.topic = topic;
		this.geofence = geofence;
	}

	public Topic getTopic() {
		return topic;
	}

	public String getGeofence() {
		return geofence;
	}
}
