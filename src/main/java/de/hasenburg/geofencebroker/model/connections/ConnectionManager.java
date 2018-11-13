package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.model.Topic;
import de.hasenburg.geofencebroker.model.spatial.Geofence;
import de.hasenburg.geofencebroker.model.spatial.Location;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ConnectionManager {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

	/*****************************************************************
	 * Connection code
	 ****************************************************************/

	public List<Connection> getActiveConnections() {
		return new ArrayList<>(connections.values());
	}

	/**
	 * Gets all subscribers for a given topic. See {@link Connection#clientIsSubscriber(Topic, Geofence, Location)} for
	 * more information on when a client is a subscriber.
	 *
	 * @param publisherLocation - can be null if publisher has not set his location yet // TODO should never be null
	 * @return a list of subscribers
	 */
	public List<Connection> getSubscribers(Topic topic, Geofence publisherGeofence, @Nullable Location publisherLocation) {
		return getActiveConnections().stream()
				.filter(connection -> connection.clientIsSubscriber(topic, publisherGeofence, publisherLocation))
				.collect(Collectors.toList());
	}

	public void putConnection(Connection connection) {
		connections.put(connection.getClientIdentifier(), connection);
	}

	public Connection getConnection(String clientIdentifier) {
		return connections.get(clientIdentifier);
	}

	public void removeConnection(String clientIdentifier) {
		connections.remove(clientIdentifier);
	}

	/*****************************************************************
	 * Generated Code
	 ****************************************************************/

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (Map.Entry<String, Connection> entry : connections.entrySet()) {
			s.append(entry.getValue().toString());
		}
		return s.toString();
	}

}
