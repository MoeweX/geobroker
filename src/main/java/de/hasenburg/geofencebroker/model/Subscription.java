package de.hasenburg.geofencebroker.model;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class Subscription {

	private final ImmutablePair<String, Integer> subscriptionId; // clientId -> unique identifier

	public Subscription(ImmutablePair<String, Integer> subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

}
