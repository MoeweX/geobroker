package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.geofence.Geofence;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class Connection {

	private static final Logger logger = LogManager.getLogger();

	private long heartbeat;
	private final String clientIdentifier;
	private Location location = null;

	private final ConcurrentHashMap<Topic, Subscription> subscriptions = new ConcurrentHashMap<>();

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

	/**
	 * Checks whether the client of this connection is a subscriber. For that:
	 * 	- the client has to be interested in the topic
	 * 	- the client has to be inside the publisher geofence
	 * 	- the publisher has to be inside the geofence the client defined for the topic
	 *
	 * @param publisherLocation - can be null if the publisher has not set a location yet
	 * @return true, if subscriber
	 */
	public boolean clientIsSubscriber(Topic topic, Geofence publisherGeofence, @Nullable Location publisherLocation) {
		boolean subscribedToTopic = subscriptions.containsKey(topic);

		// test whether this client is subscribed to the topic
		if (!subscribedToTopic) {
			return false;
		}

		Subscription subscription = subscriptions.get(topic);

		// test whether the location of this client is inside the geofence of the published message
		if (!publisherGeofence.locationInFence(location)) {
			return false;
		}

		// when there is no publisher location, the subscriber geofence has to be infinite
		if (publisherLocation == null && subscription.getGeofence().getCircleDiameterInMeter() >= 0) {
			return false;
		}

		// last check: when publisher in subscription geofence, the client is subscribed
		return subscription.getGeofence().locationInFence(publisherLocation);
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
