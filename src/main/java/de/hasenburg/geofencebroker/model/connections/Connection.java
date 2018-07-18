package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Topic;

import java.util.HashMap;

public class Connection {

	private boolean active = false;
	private String clientIdentifier;

	private final HashMap<Topic, Subscription> subscriptions = new HashMap<>();

	public Connection(String clientIdentifier) {
		this.clientIdentifier = clientIdentifier;
	}

	public void addSubscription(Subscription subscription) {
		subscriptions.put(subscription.getTopic(), subscription);
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
