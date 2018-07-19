package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Location;
import de.hasenburg.geofencebroker.model.Topic;

import java.util.HashMap;
import java.util.Optional;

public class Connection {

	private boolean active = false;
	private String clientIdentifier;
	private Location location = null;

	private final HashMap<Topic, Subscription> subscriptions = new HashMap<>();

	public Connection(String clientIdentifier) {
		this.clientIdentifier = clientIdentifier;
	}

	public void addSubscription(Subscription subscription) {
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

}
