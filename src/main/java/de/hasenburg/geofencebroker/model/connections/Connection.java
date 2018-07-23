package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Optional;

public class Connection {

	private static final Logger logger = LogManager.getLogger();

	private boolean active = false;
	private String clientIdentifier;
	private Location location = null;

	private final HashMap<Topic, Subscription> subscriptions = new HashMap<>();

	public Connection(String clientIdentifier) {
		this.clientIdentifier = clientIdentifier;
	}

	public void putSubscription(Subscription subscription) {
		subscriptions.put(subscription.getTopic(), subscription);
	}

	public void updateLocation(Location location) {
		this.location = location;
	}

	public Optional<Location> getLocation() {
		return Optional.ofNullable(location);
	}

	public String getClientIdentifier() {
		return clientIdentifier;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public boolean isActive() {
		return this.active;
	}

	// TODO add geofence check
	public boolean subscribedToTopic(Topic topic) {
		return subscriptions.containsKey(topic);
	}

	/**
	 * Checks whether a connection is active and not null -> is connected
	 * @param connection - the connection to be checked
	 * @return boolean indicating above
	 */
	public static boolean isConnected(Connection connection) {
		if (connection != null && connection.isActive()) {
			logger.trace("Received message from active client");
			return true;
		}
		logger.trace("Received message from not connected client");
		return false;
	}

	@Override
	public String toString() {
		return "Connection{" +
				"active=" + active +
				", clientIdentifier='" + clientIdentifier + '\'' +
				", location=" + location +
				", subscriptions=" + subscriptions.keySet() +
				'}';
	}
}
