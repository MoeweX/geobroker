package de.hasenburg.geofencebroker.model.connections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {

	private static final Logger logger = LogManager.getLogger();

	private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

	/*****************************************************************
	 * Connection code
	 ****************************************************************/

	public List<Connection> getActiveConnections() {
		return new ArrayList<>(connections.values());
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
