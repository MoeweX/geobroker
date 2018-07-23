package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
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

	// TODO add geofence check
	public boolean subscribedToTopic(Topic topic) {
		return subscriptions.containsKey(topic);
	}

	@Override
	public String toString() {
		return "Connection{" +
				"heartbeat=" + heartbeat +
				", clientIdentifier='" + clientIdentifier + '\'' +
				", location=" + location +
				", subscriptions=" + subscriptions.keySet() +
				'}';
	}
}
