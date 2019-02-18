package de.hasenburg.geobroker.server.storage.client;

import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientDirectory {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<String, Client> clients = new ConcurrentHashMap<>();

	/*****************************************************************
	 * Client code
	 ****************************************************************/

	/**
	 * Add a client to the directory, if it did not exist before.
	 *
	 * @param clientIdentifier of the to be added client
	 * @param initialLocation - the initial location of the client
	 * @return true, if added
	 */
	public boolean addClient(String clientIdentifier, Location initialLocation) {
		logger.trace("Connecting client {}", clientIdentifier);
		if (clients.containsKey(clientIdentifier)) {
			logger.warn("Tried to add client {}, but already existed", clientIdentifier);
			return false;
		}
		clients.put(clientIdentifier, new Client(clientIdentifier, initialLocation));
		return true;
	}

	/**
	 * Removes a client from the directory.
	 *
	 * @param clientIdentifier of the to be removed client
	 * @return true, if client existed before
	 */
	public boolean removeClient(String clientIdentifier) {
		logger.trace("Removing client {}", clientIdentifier);
		if (clients.remove(clientIdentifier) == null) {
			logger.warn("Tried to remove client, but did not exist");
			return false;
		}
		return true;
	}

	public boolean updateClientLocation(String clientIdentifier, Location location) {
		logger.trace("Updating client {} location to {}", clientIdentifier, location);
		Client c = clients.get(clientIdentifier);

		if (c == null) return false;

		c.updateLocation(location);
		return true;
	}

	public Location getClientLocation(String clientIdentifier) {
		logger.trace("Retrieving location of client {}", clientIdentifier);
		Client c = clients.get(clientIdentifier);
		if (c == null) return null;

		return c.getLocation();

	}

	public int getNumberOfClients() {
		return clients.size();
	}

	/**
	 * Checks whether a {@link Client} is subscribed to the given {@link Topic}. If so, it calculates the geofence
	 * difference (old geofence - given geofence) and returns it together with the subscription identifier of the
	 * existing {@link Subscription}. If not subscribed yet, return null
	 *
	 * !!!!!!!!!!!!!!!!!!!!!
	 *
	 * NOTE: For now, the method returns the existing subscription's original geofence and does not calculate the
	 * difference
	 *
	 * !!!!!!!!!!!!!!!!!!!!!
	 *
	 * @param clientIdentifier - the identifier of the {@link Client}
	 * @param topic - see above
	 * @param geofence - see above
	 * @return see above
	 */
	public ImmutablePair<ImmutablePair<String, Integer>, Geofence> checkIfSubscribed(String clientIdentifier,
																					 Topic topic, Geofence geofence) {
		Client c = clients.get(clientIdentifier);
		if (c == null) return null;

		Subscription s = c.getSubscription(topic);
		if (s == null) return null;

		return new ImmutablePair<>(s.getSubscriptionId(), s.getGeofence());
	}

	/**
	 * Creates a new {@link Subscription} based on the provided information and replace the existing one, if any
	 * existed. Returns the subscription id of the newly created subscription, in case an old has has been replaced, the
	 * newly created subscription has the same identifier as the replaced one.
	 *
	 * @param clientIdentifier - identifier of the {@link Client}
	 * @param topic - topic of subscription
	 * @param geofence - geofence of subscription
	 * @return the above specified subscription id
	 */
	public ImmutablePair<String, Integer> putSubscription(String clientIdentifier, Topic topic, Geofence geofence) {
		Client c = clients.get(clientIdentifier);
		if (c == null) return null;

		Subscription s = c.getSubscription(topic);
		if (s == null) return c.createAndPutSubscription(topic, geofence);

		// a subscription existed, so we need to update the geofence to the new one
		s.setGeofence(geofence);

		return s.getSubscriptionId();
	}

	/**
	 * Removes an existing {@link Subscription} from a {@link Client}, if it exists.
	 *
	 * @param clientIdentifier - identifier of the {@link Client}
	 * @param topic - topic of to be removed subscription
	 * @return the subscription id of the to be removed subscription or null, if none existed
	 */
	public ImmutablePair<String, Integer> removeSubscription(String clientIdentifier, Topic topic) {
		Client c = clients.get(clientIdentifier);
		if (c == null) return null;

		return c.removeSubscription(topic);
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (Map.Entry<String, Client> entry : clients.entrySet()) {
			s.append(entry.getValue().toString());
		}
		return s.toString();
	}

}
