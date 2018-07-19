package de.hasenburg.geofencebroker.model.connections;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.RouterMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class ConnectionManager {

	private static final Logger logger = LogManager.getLogger();

	private final HashMap<String, Connection> connections = new HashMap<>();
	private RouterCommunicator routerCommunicator;

	public ConnectionManager(RouterCommunicator router) {
		this.routerCommunicator = router;
	}

	public List<Connection> getActiveConnections() {
		return connections.values().stream().filter(c -> c.isActive()).collect(Collectors.toList());
	}

	public List<Connection> getInactiveConnections() {
		return connections.values().stream().filter(c -> !c.isActive()).collect(Collectors.toList());
	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#CONNECT} messages and updates connections accordingly.
	 *
	 * @return Optional<Connection>, is empty when invalid ControlPacketType
	 */
	public Optional<Connection> processCONNECT(RouterMessage message) {
		if (message.getControlPacketType() != ControlPacketType.CONNECT) {
			return Optional.empty();
		}

		// get connection or create new
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection == null) {
			connection = new Connection(message.getClientIdentifier());
		}

		if (connection.isActive()) {
			logger.debug("Connection of {} was already active, so protocol error. Closing now.", connection.getClientIdentifier());
			connection.setActive(false);
			routerCommunicator.sendDISCONNECT(message.getClientIdentifier(), ReasonCode.ProtocolError);
		} else {
			logger.debug("Connection of {} is now active, acknowledging.", connection.getClientIdentifier());
			connection.setActive(true);
			routerCommunicator.sendCONNACK(message.getClientIdentifier());
		}

		connections.put(connection.getClientIdentifier(), connection);
		return Optional.of(connection);
	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#DISCONNECT} messages and updates connections accordingly.
	 *
	 * @return Optional<Connection>, is empty when invalid ControlPacketType or connection for clientIdentifier does not exist
	 */
	public Optional<Connection> processDISCONNECT(RouterMessage message) {
		if (message.getControlPacketType() != ControlPacketType.DISCONNECT) {
			return Optional.empty();
		}

		// get connection
		Connection connection = connections.get(message.getClientIdentifier());
		if (connection == null) {
			return Optional.empty();
		}

		// set to inactive
		connection.setActive(false);

		connections.put(connection.getClientIdentifier(), connection);
		return Optional.of(connection);
	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#SUBSCRIBE} messages and updates connections accordingly.
	 */
	public Optional<Subscription> processSUBSCRIBEforConnection(RouterMessage message) {
		// TODO Implement
		return Optional.empty();
	}

	/**
	 * Processes {@link de.hasenburg.geofencebroker.communication.ControlPacketType#UNSUBSCRIBE} messages and updates connections accordingly.
	 */
	public Optional<Subscription> processUNSUBSCRIBEforConnection(RouterMessage message) {
		// TODO Implement
		return Optional.empty();
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("\n");
		for (Map.Entry<String, Connection> entry : connections.entrySet()) {
			s.append("\t").append(entry.getKey()).append(", is active: ").append(entry.getValue().isActive());
		}
		return s.toString();
	}
}
