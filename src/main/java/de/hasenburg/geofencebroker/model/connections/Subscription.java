package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.geofence.Geofence;

public class Subscription {

	private Topic topic;
	private Geofence geofence;

	public Subscription(Topic topic, Geofence geofence) {
		this.topic = topic;
		this.geofence = geofence;
	}

	public Topic getTopic() {
		return topic;
	}

	public Geofence getGeofence() {
		return geofence;
	}
}
