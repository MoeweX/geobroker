package de.hasenburg.geobroker.model.clients;

import de.hasenburg.geobroker.model.Topic;
import de.hasenburg.geobroker.model.spatial.Geofence;
import de.hasenburg.geobroker.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

	private static final Logger logger = LogManager.getLogger();

	private long heartbeat;
	private final String clientIdentifier; // every clientId may only exist once
	private Location location;
	private AtomicInteger lastSubscriptionId = new AtomicInteger(0);

	private final ConcurrentHashMap<Topic, Subscription> subscriptions = new ConcurrentHashMap<>();

	protected Client(String clientIdentifier, Location location) {
		updateHeartbeat();
		this.location = location;
		this.clientIdentifier = clientIdentifier;
	}

	protected String getClientIdentifier() {
		return clientIdentifier;
	}

	/*****************************************************************
	 * Location
	 ****************************************************************/

	protected void updateLocation(Location location) {
		updateHeartbeat();
		this.location = location;
	}

	protected Location getLocation() {
		return location;
	}

	/*****************************************************************
	 * Subscription
	 ****************************************************************/

	protected ImmutablePair<String, Integer> createAndPutSubscription(Topic topic, Geofence geofence) {
		updateHeartbeat();
		Subscription s = new Subscription(new ImmutablePair<>(clientIdentifier, lastSubscriptionId.incrementAndGet()), topic, geofence);
		subscriptions.put(topic, s);
		return s.getSubscriptionId();
	}

	protected @Nullable Subscription getSubscription(Topic topic) {
		return subscriptions.get(topic);
	}

	protected @Nullable ImmutablePair<String, Integer> removeSubscription(Topic topic) {
		Subscription s = subscriptions.get(topic);
		if (s == null) return null;
		return s.getSubscriptionId();
	}

	/*****************************************************************
	 * Others
	 ****************************************************************/

	private void updateHeartbeat() {
		heartbeat = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("\n");
		for (Subscription subscription : subscriptions.values()) {
			s.append(subscription.toString()).append(", ");
		}

		return "Client{" +
				"heartbeat=" + heartbeat +
				", clientIdentifier='" + clientIdentifier + '\'' +
				", location=" + location +
				", subscriptions=" + s +
				'}';
	}
}
