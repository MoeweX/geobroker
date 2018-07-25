package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Connection {

	private static final Logger logger = LogManager.getLogger();

	private long heartbeat;
	private boolean active = false;
	private String clientIdentifier;
	private Location location = null;

	private final HashMap<Topic, Subscription> subscriptions = new HashMap<>();

	public Connection(String clientIdentifier) {
		updateHeartbeat();
		this.clientIdentifier = clientIdentifier;
	}

	public void putSubscription(Subscription subscription) {
		updateHeartbeat();
		subscriptions.put(subscription.getTopic(), subscription);
	}

	public void updateLocation(Location location) {
		updateHeartbeat();
		this.location = location;
	}

	public Optional<Location> getLocation() {
		return Optional.ofNullable(location);
	}

	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public void updateHeartbeat() {
		heartbeat = System.currentTimeMillis();
	}

	@SuppressWarnings({"RedundantIfStatement", "OptionalUsedAsFieldOrParameterType"})
	public boolean shouldGetMessage(Topic topic, Geofence publisherGeofence, Optional<Location> publisherLocation) {
		boolean subscribedToTopic = subscriptions.containsKey(topic);

		// test whether this client is subscribed to the topic
		if (!subscribedToTopic) {
			return false;
		}

		// test whether the location of this client is inside the geofence of the published message
		if (!publisherGeofence.locationInFence(location)) {
			return false;
		}

		// test whether the location of the publisher is inside the geofence of the subscription
		if (publisherLocation.isPresent()) {
			if (!subscriptions.get(topic).getGeofence().locationInFence(publisherLocation.get())) {
				return false;
			}
		} else {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("\n");
		for (Map.Entry<Topic, Subscription> entry : subscriptions.entrySet()) {
			s.append(entry.getValue().toString());
		}

		return "Connection{" +
				"heartbeat=" + heartbeat +
				", clientIdentifier='" + clientIdentifier + '\'' +
				", location=" + location +
				", subscriptions=" + s +
				'}';
	}
}
